"""
scheduler.py

한국환경공단_전기자동차 충전소 정보 API에서 서울(zcode=11) 데이터를 수집하여
원격 MongoDB에 저장하는 단일 파일 수집기입니다.

지원 명령어:
    python scheduler.py collect-master
    python scheduler.py collect-status-once
    python scheduler.py generate-stats
    python scheduler.py run

Python 3.11+ 기준으로 작성되었습니다.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import unquote

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError, PyMongoError


# -----------------------------------------------------------------------------
# 기본 상수
# -----------------------------------------------------------------------------

BASE_URL = "http://apis.data.go.kr/B552584/EvCharger"
KST = timezone(timedelta(hours=9))


# -----------------------------------------------------------------------------
# 로그 설정
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# 설정 로드
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """환경변수에서 읽어 온 설정값을 담는 객체입니다."""

    service_key: str
    mongodb_uri: str
    mongodb_db_name: str = "ev_charger"
    seoul_zcode: str = "11"
    num_of_rows: int = 9999
    status_period_minutes: int = 10
    scheduler_interval_minutes: int = 10
    refresh_master_on_start: bool = False
    master_daily_refresh_enabled: bool = True
    master_refresh_hour_kst: int = 4
    request_timeout_seconds: int = 30
    max_retries: int = 4
    retry_base_sleep_seconds: float = 1.5


CONFIG: Config | None = None


def parse_bool(value: str | None, default: bool = False) -> bool:
    """문자열 환경변수를 bool로 변환합니다."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config() -> Config:
    """
    .env 파일 및 서버 환경변수에서 설정을 읽습니다.

    주의:
    - API 키는 코드에 직접 작성하지 않습니다.
    - 공공데이터포털 키를 'Decoding' 키로 저장하는 것을 권장합니다.
    - 혹시 URL Encoding 키를 넣어도 동작 가능성을 높이기 위해 unquote()를 1회 적용합니다.
    """
    global CONFIG

    load_dotenv()

    raw_service_key = os.getenv("DATA_GO_KR_SERVICE_KEY")
    mongodb_uri = os.getenv("MONGODB_URI")

    if not raw_service_key:
        raise RuntimeError("환경변수 DATA_GO_KR_SERVICE_KEY가 비어 있습니다.")
    if not mongodb_uri:
        raise RuntimeError("환경변수 MONGODB_URI가 비어 있습니다.")

    # requests의 params를 사용하면 자동으로 URL 인코딩됩니다.
    # 이미 인코딩된 키를 그대로 넣으면 이중 인코딩될 수 있어 unquote를 한 번 적용합니다.
    service_key = unquote(raw_service_key.strip())

    CONFIG = Config(
        service_key=service_key,
        mongodb_uri=mongodb_uri.strip(),
        mongodb_db_name=os.getenv("MONGODB_DB_NAME", "ev_charger").strip(),
        seoul_zcode=os.getenv("SEOUL_ZCODE", "11").strip(),
        num_of_rows=int(os.getenv("NUM_OF_ROWS", "9999")),
        status_period_minutes=int(os.getenv("STATUS_PERIOD_MINUTES", "10")),
        scheduler_interval_minutes=int(os.getenv("SCHEDULER_INTERVAL_MINUTES", "10")),
        refresh_master_on_start=parse_bool(os.getenv("REFRESH_MASTER_ON_START"), False),
        master_daily_refresh_enabled=parse_bool(os.getenv("MASTER_DAILY_REFRESH_ENABLED"), True),
        master_refresh_hour_kst=int(os.getenv("MASTER_REFRESH_HOUR_KST", "4")),
    )

    if CONFIG.num_of_rows < 10 or CONFIG.num_of_rows > 9999:
        raise RuntimeError("NUM_OF_ROWS는 10 이상 9999 이하로 설정해주세요.")
    if CONFIG.status_period_minutes < 1 or CONFIG.status_period_minutes > 10:
        raise RuntimeError("STATUS_PERIOD_MINUTES는 1 이상 10 이하로 설정해주세요.")
    if CONFIG.scheduler_interval_minutes < 1:
        raise RuntimeError("SCHEDULER_INTERVAL_MINUTES는 1 이상이어야 합니다.")
    if CONFIG.master_refresh_hour_kst < 0 or CONFIG.master_refresh_hour_kst > 23:
        raise RuntimeError("MASTER_REFRESH_HOUR_KST는 0 이상 23 이하로 설정해주세요.")

    logger.info(
        "설정 로드 완료: db=%s, zcode=%s, numOfRows=%s, statusPeriod=%s분, interval=%s분, refreshMasterOnStart=%s, masterDailyRefresh=%s, masterRefreshHourKst=%s",
        CONFIG.mongodb_db_name,
        CONFIG.seoul_zcode,
        CONFIG.num_of_rows,
        CONFIG.status_period_minutes,
        CONFIG.scheduler_interval_minutes,
        CONFIG.refresh_master_on_start,
        CONFIG.master_daily_refresh_enabled,
        CONFIG.master_refresh_hour_kst,
    )
    return CONFIG


def require_config() -> Config:
    """CONFIG가 없으면 로드하고, 있으면 그대로 반환합니다."""
    return CONFIG if CONFIG is not None else load_config()


# -----------------------------------------------------------------------------
# MongoDB 연결 및 인덱스
# -----------------------------------------------------------------------------


def get_mongo_client() -> MongoClient:
    """MongoDB 클라이언트를 만들고 ping으로 연결을 확인합니다."""
    config = require_config()
    client = MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    logger.info("MongoDB 연결 성공")
    return client


def create_indexes(db: Database) -> None:
    """중복 방지 및 조회 속도 향상을 위한 인덱스를 생성합니다."""
    logger.info("MongoDB 인덱스 생성 시작")

    db.charger_master.create_index(
        [("statId", 1), ("chgerId", 1)],
        unique=True,
        name="uq_master_statId_chgerId",
    )
    db.charger_master.create_index([("zcode", 1)], name="idx_master_zcode")
    db.charger_master.create_index([("updatedAt", -1)], name="idx_master_updatedAt")

    db.charger_status_snapshot.create_index(
        [("statId", 1), ("chgerId", 1), ("collectedAtBucket", 1)],
        unique=True,
        name="uq_status_statId_chgerId_bucket",
    )
    db.charger_status_snapshot.create_index(
        [("collectedAt", -1)],
        name="idx_status_collectedAt",
    )
    db.charger_status_snapshot.create_index(
        [("collectedAtBucket", -1)],
        name="idx_status_collectedAtBucket",
    )
    db.charger_status_snapshot.create_index(
        [("zcode", 1), ("stat", 1)],
        name="idx_status_zcode_stat",
    )

    db.charger_current.create_index(
        [("statId", 1), ("chgerId", 1)],
        unique=True,
        name="uq_current_statId_chgerId",
    )
    db.charger_current.create_index([("stat", 1)], name="idx_current_stat")
    db.charger_current.create_index([("zcode", 1), ("stat", 1)], name="idx_current_zcode_stat")
    db.charger_current.create_index([("lastObservedAt", -1)], name="idx_current_lastObservedAt")
    db.charger_current.create_index([("eventAt", -1)], name="idx_current_eventAt")

    db.charger_stats.create_index(
        [("type", 1), ("collectedAtBucket", -1)],
        name="idx_stats_type_bucket",
    )

    db.charger_current.create_index([("updatedAt", -1)], name="idx_current_updatedAt")

    db.charger_stats_cache.create_index(
        [("type", 1), ("scopeKey", 1)],
        unique=True,
        name="uq_stats_cache_type_scopeKey",
    )
    db.charger_stats_cache.create_index([("generatedAt", -1)], name="idx_stats_cache_generatedAt")
    db.charger_stats_cache.create_index([("sourceBucket", -1)], name="idx_stats_cache_sourceBucket")

    logger.info("MongoDB 인덱스 생성 완료")


# -----------------------------------------------------------------------------
# 시간 유틸리티
# -----------------------------------------------------------------------------


def utc_now() -> datetime:
    """현재 UTC 시간을 timezone-aware datetime으로 반환합니다."""
    return datetime.now(timezone.utc)


def to_kst_string(dt: datetime) -> str:
    """UTC datetime을 사람이 보기 쉬운 KST ISO 문자열로 변환합니다."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).isoformat(timespec="seconds")


def floor_datetime_to_minutes(dt: datetime, minutes: int) -> datetime:
    """
    시간을 N분 단위로 내림 처리합니다.

    예: 10분 단위인 경우
    12:03 -> 12:00
    12:19 -> 12:10
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    floored_minute = (dt.minute // minutes) * minutes
    return dt.replace(minute=floored_minute, second=0, microsecond=0)


def parse_ev_datetime(value: Any) -> datetime | None:
    """
    API의 yyyymmddHHMMSS 문자열을 UTC datetime으로 변환합니다.

    예: 20260504205004 -> 2026-05-04 20:50:04 KST -> UTC datetime
    """
    text = clean_str(value)
    if text is None:
        return None
    try:
        dt_kst = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=KST)
        return dt_kst.astimezone(timezone.utc)
    except ValueError:
        return None


# -----------------------------------------------------------------------------
# XML/API 유틸리티
# -----------------------------------------------------------------------------


def strip_namespace(tag: str) -> str:
    """XML 태그에서 namespace를 제거합니다."""
    return tag.split("}", 1)[-1] if "}" in tag else tag


def element_to_dict(element: ET.Element) -> dict[str, Any]:
    """XML <item> 하위 태그를 dict로 바꿉니다."""
    result: dict[str, Any] = {}
    for child in list(element):
        key = strip_namespace(child.tag)
        value = child.text.strip() if child.text else None
        result[key] = value
    return result


def find_text(root: ET.Element, tag_name: str) -> str | None:
    """XML 전체에서 특정 태그명을 찾아 text를 반환합니다."""
    for element in root.iter():
        if strip_namespace(element.tag) == tag_name:
            return element.text.strip() if element.text else None
    return None


def parse_xml_root(xml_text: str) -> ET.Element:
    """XML 문자열을 ElementTree root로 파싱합니다."""
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError as exc:
        # API 장애나 인증 오류 상황에서 XML이 아닌 HTML/텍스트가 올 수도 있습니다.
        preview = xml_text[:300].replace("\n", " ")
        raise RuntimeError(f"XML 파싱 실패: {exc}; 응답 미리보기={preview}") from exc


def check_api_result_or_raise(xml_text: str) -> None:
    """
    공공데이터포털 API 응답의 오류 코드를 확인합니다.

    정상 응답은 보통 resultCode=00입니다.
    인증 오류 등은 OpenAPI_ServiceResponse 형태로 올 수 있습니다.
    """
    root = parse_xml_root(xml_text)

    # 일반 응답: <resultCode>00</resultCode>, <resultMsg>OK</resultMsg>
    result_code = find_text(root, "resultCode")
    result_msg = find_text(root, "resultMsg")

    if result_code and result_code not in {"00", "NORMAL_CODE"}:
        raise RuntimeError(f"API 오류: resultCode={result_code}, resultMsg={result_msg}")

    # 공공데이터포털 인증/트래픽 오류 응답에서 자주 등장하는 태그입니다.
    return_reason_code = find_text(root, "returnReasonCode")
    return_auth_msg = find_text(root, "returnAuthMsg")
    err_msg = find_text(root, "errMsg")

    if return_reason_code or return_auth_msg or err_msg:
        raise RuntimeError(
            f"API 인증/서비스 오류: returnReasonCode={return_reason_code}, "
            f"returnAuthMsg={return_auth_msg}, errMsg={err_msg}"
        )


def request_api(endpoint: str, params: dict[str, Any]) -> str:
    """
    API를 호출합니다.

    - ServiceKey는 여기에서만 추가합니다.
    - 로그에는 API 키를 절대 출력하지 않습니다.
    - 네트워크 오류, 5xx, API 오류에 대해 exponential backoff retry를 수행합니다.
    """
    config = require_config()
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"

    request_params = {
        "ServiceKey": config.service_key,
        **params,
    }

    safe_params = {k: v for k, v in request_params.items() if k != "ServiceKey"}

    last_error: Exception | None = None
    for attempt in range(1, config.max_retries + 1):
        try:
            logger.info("API 요청: endpoint=%s, params=%s, attempt=%s", endpoint, safe_params, attempt)
            response = requests.get(url, params=request_params, timeout=config.request_timeout_seconds)

            # HTTP 상태 코드가 4xx/5xx이면 예외 발생
            response.raise_for_status()

            # XML 내부의 API 오류 코드 확인
            check_api_result_or_raise(response.text)
            return response.text

        except (requests.RequestException, RuntimeError) as exc:
            last_error = exc
            if attempt >= config.max_retries:
                break

            sleep_seconds = config.retry_base_sleep_seconds * (2 ** (attempt - 1))
            logger.warning(
                "API 요청 실패 후 재시도 예정: endpoint=%s, params=%s, error=%s, sleep=%.1fs",
                endpoint,
                safe_params,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"API 요청 최종 실패: endpoint={endpoint}, params={safe_params}, error={last_error}")


def parse_xml_items(xml_text: str) -> list[dict[str, Any]]:
    """API XML 응답에서 <item> 목록을 추출합니다."""
    root = parse_xml_root(xml_text)
    items: list[dict[str, Any]] = []

    for element in root.iter():
        if strip_namespace(element.tag) == "item":
            items.append(element_to_dict(element))

    return items


def parse_total_count(xml_text: str) -> int | None:
    """API XML 응답에서 totalCount를 추출합니다."""
    root = parse_xml_root(xml_text)
    value = find_text(root, "totalCount")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def fetch_paginated(endpoint: str, extra_params: dict[str, Any]) -> list[dict[str, Any]]:
    """
    pageNo, numOfRows를 사용해 모든 페이지를 가져옵니다.

    numOfRows가 최대 9999로 제한되므로, totalCount가 더 크면 pageNo를 증가시키며 반복합니다.
    """
    config = require_config()
    all_items: list[dict[str, Any]] = []
    page_no = 1
    total_count: int | None = None

    while True:
        params = {
            "pageNo": page_no,
            "numOfRows": config.num_of_rows,
            **extra_params,
        }

        xml_text = request_api(endpoint, params)
        page_items = parse_xml_items(xml_text)
        page_total_count = parse_total_count(xml_text)
        if page_total_count is not None:
            total_count = page_total_count

        all_items.extend(page_items)

        logger.info(
            "페이지 수집 완료: endpoint=%s, pageNo=%s, pageItems=%s, accumulated=%s, totalCount=%s",
            endpoint,
            page_no,
            len(page_items),
            len(all_items),
            total_count,
        )

        # 종료 조건 1: 이번 페이지에 데이터가 없으면 더 가져올 것이 없다고 봅니다.
        if not page_items:
            break

        # 종료 조건 2: totalCount 기준으로 전체를 다 가져왔으면 종료합니다.
        if total_count is not None and len(all_items) >= total_count:
            break

        # 종료 조건 3: totalCount가 없더라도 이번 페이지가 꽉 차지 않았다면 마지막 페이지로 봅니다.
        if len(page_items) < config.num_of_rows:
            break

        page_no += 1

        # 안전장치: API 이상으로 무한 루프가 생기는 것을 방지합니다.
        if page_no > 10_000:
            raise RuntimeError("페이지네이션이 비정상적으로 길어져 중단합니다.")

    return all_items


# -----------------------------------------------------------------------------
# 데이터 정규화
# -----------------------------------------------------------------------------


def clean_str(value: Any) -> str | None:
    """빈 문자열을 None으로 정리합니다."""
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def to_float(value: Any) -> float | None:
    """문자열 숫자를 float로 변환합니다. 실패하면 None을 반환합니다."""
    text = clean_str(value)
    if text is None:
        return None
    try:
        if text.lower() in {"nan", "null", "none"}:
            return None
        return float(text)
    except ValueError:
        return None


def normalize_master_item(item: dict[str, Any]) -> dict[str, Any] | None:
    """
    충전소/충전기 기본정보 item을 MongoDB 저장용 dict로 변환합니다.

    statId + chgerId가 논리적 고유 키이므로 둘 중 하나라도 없으면 저장하지 않습니다.
    """
    now = utc_now()
    stat_id = clean_str(item.get("statId"))
    chger_id = clean_str(item.get("chgerId"))

    if not stat_id or not chger_id:
        logger.warning("기본정보 item에 statId 또는 chgerId가 없어 건너뜁니다: %s", item)
        return None

    return {
        "statNm": clean_str(item.get("statNm")),
        "statId": stat_id,
        "chgerId": chger_id,
        "chgerType": clean_str(item.get("chgerType")),
        "addr": clean_str(item.get("addr")),
        "addrDetail": clean_str(item.get("addrDetail")),
        "lat": to_float(item.get("lat")),
        "lng": to_float(item.get("lng")),
        "useTime": clean_str(item.get("useTime")),
        "busiId": clean_str(item.get("busiId")),
        "busiNm": clean_str(item.get("busiNm")),
        "busiCall": clean_str(item.get("busiCall")),
        "output": clean_str(item.get("output")),
        "method": clean_str(item.get("method")),
        "zcode": clean_str(item.get("zcode")),
        "parkingFree": clean_str(item.get("parkingFree")),
        "limitYn": clean_str(item.get("limitYn")),
        "limitDetail": clean_str(item.get("limitDetail")),
        "delYn": clean_str(item.get("delYn")),
        "kind": clean_str(item.get("kind")),
        "kindDetail": clean_str(item.get("kindDetail")),
        "updatedAt": now,
        "raw": item,
    }


def normalize_status_item(
    item: dict[str, Any],
    collected_at: datetime,
    collected_at_bucket: datetime,
) -> dict[str, Any] | None:
    """
    충전기 상태 item을 MongoDB 저장용 dict로 변환합니다.

    statId + chgerId + collectedAtBucket 조합으로 중복 저장을 방지합니다.
    """
    config = require_config()
    stat_id = clean_str(item.get("statId"))
    chger_id = clean_str(item.get("chgerId"))

    if not stat_id or not chger_id:
        logger.warning("상태정보 item에 statId 또는 chgerId가 없어 건너뜁니다: %s", item)
        return None

    return {
        "statId": stat_id,
        "chgerId": chger_id,
        "busiId": clean_str(item.get("busiId")),
        "stat": clean_str(item.get("stat")),
        "statUpdDt": clean_str(item.get("statUpdDt")),
        "eventAt": parse_ev_datetime(item.get("statUpdDt")),
        "zcode": clean_str(item.get("zcode")) or config.seoul_zcode,
        "collectedAt": collected_at,
        "collectedAtKst": to_kst_string(collected_at),
        "collectedAtBucket": collected_at_bucket,
        "sourcePeriodMinutes": config.status_period_minutes,
        "raw": item,
    }



# -----------------------------------------------------------------------------
# charger_current 갱신 로직
# -----------------------------------------------------------------------------


def build_current_doc_from_status_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """getChargerStatus 결과를 charger_current 저장용 문서로 변환합니다."""
    observed_at = doc["collectedAt"]
    return {
        "statId": doc["statId"],
        "chgerId": doc["chgerId"],
        "busiId": doc.get("busiId"),
        "stat": doc.get("stat"),
        "statUpdDt": doc.get("statUpdDt"),
        "eventAt": doc.get("eventAt") or observed_at,
        "zcode": doc.get("zcode"),
        "lastObservedAt": observed_at,
        "lastObservedAtKst": to_kst_string(observed_at),
        "lastSnapshotBucket": doc.get("collectedAtBucket"),
        "sourcePeriodMinutes": doc.get("sourcePeriodMinutes"),
        "currentSource": "getChargerStatus",
        "raw": doc.get("raw"),
        "updatedAt": utc_now(),
    }


def build_current_doc_from_master_doc(doc: dict[str, Any], collected_at: datetime) -> dict[str, Any]:
    """
    getChargerInfo 결과로 charger_current 초기 문서를 만듭니다.

    주의: master 수집은 현재 상태 컬렉션을 '초기 생성'하는 용도입니다.
    이미 charger_current에 있는 문서는 상태 스케줄러가 더 최신일 수 있으므로 덮어쓰지 않습니다.
    """
    raw = doc.get("raw") or {}
    stat_upd_dt = clean_str(raw.get("statUpdDt"))
    return {
        "statId": doc["statId"],
        "chgerId": doc["chgerId"],
        "busiId": doc.get("busiId"),
        "stat": clean_str(raw.get("stat")),
        "statUpdDt": stat_upd_dt,
        "eventAt": parse_ev_datetime(stat_upd_dt) or collected_at,
        "zcode": doc.get("zcode"),
        "lastObservedAt": collected_at,
        "lastObservedAtKst": to_kst_string(collected_at),
        "sourcePeriodMinutes": None,
        "currentSource": "getChargerInfo_initial",
        "raw": {
            "busiId": doc.get("busiId"),
            "statId": doc["statId"],
            "chgerId": doc["chgerId"],
            "stat": clean_str(raw.get("stat")),
            "statUpdDt": stat_upd_dt,
            "lastTsdt": clean_str(raw.get("lastTsdt")),
            "lastTedt": clean_str(raw.get("lastTedt")),
            "nowTsdt": clean_str(raw.get("nowTsdt")),
        },
        "updatedAt": utc_now(),
    }


def upsert_current_status(db: Database, status_docs: list[dict[str, Any]]) -> dict[str, Any]:
    """상태 수집 결과를 충전기별 최신 상태 컬렉션에 upsert합니다."""
    operations: list[UpdateOne] = []

    for doc in status_docs:
        current_doc = build_current_doc_from_status_doc(doc)
        operations.append(
            UpdateOne(
                {"statId": current_doc["statId"], "chgerId": current_doc["chgerId"]},
                {
                    "$set": current_doc,
                    "$setOnInsert": {"createdAt": current_doc["lastObservedAt"]},
                },
                upsert=True,
            )
        )

    result_summary = {"operationCount": len(operations), "matched": 0, "modified": 0, "upserted": 0}
    if operations:
        try:
            result = db.charger_current.bulk_write(operations, ordered=False)
            result_summary.update(
                {
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                    "upserted": result.upserted_count,
                }
            )
        except BulkWriteError as exc:
            logger.exception("현재상태 bulk_write 실패: %s", exc.details)
            raise

    logger.info("현재상태 갱신 완료: %s", result_summary)
    return result_summary


# -----------------------------------------------------------------------------
# 수집 로직
# -----------------------------------------------------------------------------


def collect_master(db: Database) -> dict[str, Any]:
    """서울 충전소/충전기 기본정보를 1회 수집하여 charger_master에 upsert합니다."""
    config = require_config()
    started_at = utc_now()
    logger.info("기본정보 수집 시작: zcode=%s", config.seoul_zcode)

    raw_items = fetch_paginated(
        endpoint="getChargerInfo",
        extra_params={"zcode": config.seoul_zcode},
    )

    operations: list[UpdateOne] = []
    current_operations: list[UpdateOne] = []
    skipped = 0

    for raw_item in raw_items:
        doc = normalize_master_item(raw_item)
        if doc is None:
            skipped += 1
            continue

        operations.append(
            UpdateOne(
                {"statId": doc["statId"], "chgerId": doc["chgerId"]},
                {
                    "$set": doc,
                    "$setOnInsert": {"createdAt": started_at},
                },
                upsert=True,
            )
        )

        current_doc = build_current_doc_from_master_doc(doc, started_at)
        current_operations.append(
            UpdateOne(
                {"statId": current_doc["statId"], "chgerId": current_doc["chgerId"]},
                {"$setOnInsert": {**current_doc, "createdAt": started_at}},
                upsert=True,
            )
        )

    result_summary = {
        "rawCount": len(raw_items),
        "operationCount": len(operations),
        "skipped": skipped,
        "matched": 0,
        "modified": 0,
        "upserted": 0,
        "currentSeedMatched": 0,
        "currentSeedUpserted": 0,
    }

    if operations:
        try:
            result = db.charger_master.bulk_write(operations, ordered=False)
            result_summary.update(
                {
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                    "upserted": result.upserted_count,
                }
            )
        except BulkWriteError as exc:
            logger.exception("기본정보 bulk_write 실패: %s", exc.details)
            raise

    if current_operations:
        try:
            current_result = db.charger_current.bulk_write(current_operations, ordered=False)
            result_summary.update(
                {
                    "currentSeedMatched": current_result.matched_count,
                    "currentSeedUpserted": current_result.upserted_count,
                }
            )
        except BulkWriteError as exc:
            logger.exception("현재상태 초기 문서 bulk_write 실패: %s", exc.details)
            raise

    logger.info("기본정보 수집 종료: %s", result_summary)
    return result_summary


def collect_status_once(db: Database) -> dict[str, Any]:
    """
    서울 충전기 상태정보를 1회 수집하여 charger_status_snapshot에 저장합니다.

    수집이 끝난 뒤 같은 bucket 기준으로 charger_stats 기본 통계도 생성합니다.
    """
    config = require_config()
    collected_at = utc_now()
    collected_at_bucket = floor_datetime_to_minutes(collected_at, config.scheduler_interval_minutes)

    logger.info(
        "상태정보 수집 시작: zcode=%s, period=%s, bucket=%s",
        config.seoul_zcode,
        config.status_period_minutes,
        collected_at_bucket.isoformat(),
    )

    raw_items = fetch_paginated(
        endpoint="getChargerStatus",
        extra_params={
            "zcode": config.seoul_zcode,
            "period": config.status_period_minutes,
        },
    )

    operations: list[UpdateOne] = []
    status_docs: list[dict[str, Any]] = []
    skipped = 0

    for raw_item in raw_items:
        doc = normalize_status_item(raw_item, collected_at, collected_at_bucket)
        if doc is None:
            skipped += 1
            continue

        status_docs.append(doc)

        # 같은 충전기가 같은 10분 bucket에 이미 저장되어 있으면 새로 쓰지 않습니다.
        # 그래서 $set이 아니라 $setOnInsert만 사용합니다.
        operations.append(
            UpdateOne(
                {
                    "statId": doc["statId"],
                    "chgerId": doc["chgerId"],
                    "collectedAtBucket": doc["collectedAtBucket"],
                },
                {"$setOnInsert": doc},
                upsert=True,
            )
        )

    result_summary = {
        "rawCount": len(raw_items),
        "operationCount": len(operations),
        "skipped": skipped,
        "matchedDuplicate": 0,
        "insertedNew": 0,
        "bucket": collected_at_bucket.isoformat(),
    }

    if operations:
        try:
            result = db.charger_status_snapshot.bulk_write(operations, ordered=False)
            result_summary.update(
                {
                    "matchedDuplicate": result.matched_count,
                    "insertedNew": result.upserted_count,
                }
            )
        except BulkWriteError as exc:
            logger.exception("상태정보 bulk_write 실패: %s", exc.details)
            raise

    logger.info("상태정보 수집 종료: %s", result_summary)

    try:
        current_result = upsert_current_status(db, status_docs)
        result_summary["current"] = current_result
    except Exception:
        logger.exception("charger_current 갱신 실패 (계속 진행)")

    generate_precomputed_stats(db, collected_at, collected_at_bucket)

    stats_result = generate_basic_stats(db, collected_at=collected_at)
    current_stats_result = generate_current_stats(db, collected_at=collected_at)
    result_summary["stats"] = stats_result
    result_summary["currentStats"] = current_stats_result
    return result_summary


# -----------------------------------------------------------------------------
# charger_current 갱신 및 통계 사전 계산
# -----------------------------------------------------------------------------

_SCHED_SLOW_CHGER: frozenset[str] = frozenset({"02", "08"})
_SCHED_FAST_CHGER: frozenset[str] = frozenset({"01", "03", "04", "05", "06", "07", "09", "10"})
_SCHED_FAULT: frozenset[str] = frozenset({"1", "4", "5"})
_SCHED_STATUS_LABELS: dict[str, str] = {
    "0": "알수없음", "1": "통신이상", "2": "사용가능",
    "3": "충전중", "4": "운영중지", "5": "점검중", "9": "알수없음",
}
_FAST_OCC_THRESHOLD = 60         # 급속 장기 점유 기준: 60분
_SLOW_OCC_THRESHOLD = 14 * 60   # 완속 장기 점유 기준: 840분
_FAST_OCC_MAX = 4 * 60          # 급속 최대 인정 점유: 240분
_SLOW_OCC_MAX = 48 * 60         # 완속 최대 인정 점유: 2880분


def _sched_speed(chger_type: str | None, output_str: str | None) -> str:
    if chger_type in _SCHED_SLOW_CHGER:
        return "slow"
    if chger_type in _SCHED_FAST_CHGER:
        return "fast"
    v = to_float(output_str)
    return "fast" if (v is not None and v >= 50) else "slow"


def _sched_extract_gu(addr: str | None) -> str | None:
    if not addr:
        return None
    for part in str(addr).split():
        if part.endswith("구") and len(part) > 1:
            return part
    return None


def _sched_extract_dong(addr: str | None) -> str | None:
    if not addr:
        return None
    text = str(addr)
    for chunk in re.findall(r"\(([^)]*)\)", text):
        for token in re.split(r"[\s,]+", chunk):
            token = token.strip()
            if re.search(r"(동|가)$", token) and len(token) > 1:
                return token
    for token in re.split(r"[\s,()]+", text):
        token = token.strip()
        if re.search(r"(동|가)$", token) and len(token) > 1:
            return token
    return None


def _sched_parse_kst_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value).strip(), "%Y%m%d%H%M%S").replace(tzinfo=KST)
    except ValueError:
        return None


def _sched_long_occ_item(row: dict[str, Any], ref_kst: datetime) -> dict[str, Any] | None:
    if row.get("stat") != "3":
        return None
    raw = row.get("rawStatus") or {}
    last_tsdt = _sched_parse_kst_dt(raw.get("lastTsdt"))
    last_tedt = _sched_parse_kst_dt(raw.get("lastTedt"))
    if last_tsdt and last_tedt and last_tedt > last_tsdt:
        return None
    started_at = _sched_parse_kst_dt(raw.get("nowTsdt"))
    if not started_at:
        return None
    duration = int((ref_kst - started_at).total_seconds() // 60)
    speed = row.get("speed", "slow")
    threshold = _FAST_OCC_THRESHOLD if speed == "fast" else _SLOW_OCC_THRESHOLD
    max_occ = _FAST_OCC_MAX if speed == "fast" else _SLOW_OCC_MAX
    if duration < threshold or duration > max_occ:
        return None
    return {
        "statId": row.get("statId"),
        "chgerId": row.get("chgerId"),
        "statNm": row.get("statNm"),
        "addr": row.get("addr"),
        "gu": row.get("gu"),
        "dong": row.get("dong"),
        "output": row.get("output"),
        "speedType": speed,
        "nowTsdt": raw.get("nowTsdt"),
        "durationMinutes": max(duration, 0),
        "lat": row.get("lat"),
        "lng": row.get("lng"),
    }


def _sched_compute_overview(
    rows: list[dict[str, Any]],
    ref_kst: datetime,
    updated_at_kst: str | None,
    gu: str | None = None,
    dong: str | None = None,
) -> dict[str, Any]:
    total = len(rows)
    stations: set[str] = set()
    available = charging = fault = maintenance = stopped = comm_err = unknown = 0
    rapid = slow = ultra = 0
    outputs: list[float] = []
    long_occ: list[dict[str, Any]] = []
    status_dist: dict[str, int] = {}
    charger_type_dist: dict[str, int] = {}
    facility_dist: dict[str, int] = {}

    for row in rows:
        sid = row.get("statId")
        if sid:
            stations.add(sid)

        stat = row.get("stat")
        if stat == "2":
            available += 1
        elif stat == "3":
            charging += 1
        elif stat == "1":
            comm_err += 1
        elif stat == "4":
            stopped += 1
        elif stat == "5":
            maintenance += 1
        else:
            unknown += 1

        if stat in _SCHED_FAULT:
            fault += 1

        label = _SCHED_STATUS_LABELS.get(stat or "", "알수없음")
        status_dist[label] = status_dist.get(label, 0) + 1

        out = row.get("outputVal") or 0.0
        if out >= 200:
            ultra += 1
            charger_type_dist["초급속"] = charger_type_dist.get("초급속", 0) + 1
        elif out >= 50:
            rapid += 1
            charger_type_dist["급속"] = charger_type_dist.get("급속", 0) + 1
        else:
            slow += 1
            charger_type_dist["완속"] = charger_type_dist.get("완속", 0) + 1
        if out > 0:
            outputs.append(out)

        fk = row.get("kind") or row.get("kindDetail") or "기타"
        facility_dist[str(fk)] = facility_dist.get(str(fk), 0) + 1

        item = _sched_long_occ_item(row, ref_kst)
        if item:
            long_occ.append(item)

    long_occ.sort(key=lambda x: x["durationMinutes"], reverse=True)
    fast_items = [i for i in long_occ if i["speedType"] == "fast"]
    slow_items = [i for i in long_occ if i["speedType"] == "slow"]

    return {
        "scope": {"gu": gu, "dong": dong},
        "updatedAt": updated_at_kst,
        "totalChargers": total,
        "totalStations": len(stations),
        "rapidChargers": rapid,
        "slowChargers": slow,
        "ultraFastChargers": ultra,
        "availableCount": available,
        "chargingCount": charging,
        "faultCount": fault,
        "maintenanceCount": maintenance,
        "stoppedCount": stopped,
        "communicationErrorCount": comm_err,
        "unknownCount": unknown,
        "availabilityRate": round(available / total * 100, 2) if total else 0,
        "avgOutput": round(sum(outputs) / len(outputs), 2) if outputs else 0,
        "maxOutput": max(outputs) if outputs else 0,
        "longOccupancy": {
            "count": len(long_occ),
            "fastCount": len(fast_items),
            "slowCount": len(slow_items),
            "thresholdMinutes": None,
            "thresholdMinutesByType": {
                "급속": _FAST_OCC_THRESHOLD,
                "완속": _SLOW_OCC_THRESHOLD,
            },
            "items": long_occ[:20],
            "fastItems": fast_items[:20],
            "slowItems": slow_items[:20],
        },
        "statusDistribution": status_dist,
        "chargerTypeDistribution": charger_type_dist,
        "facilityDistribution": facility_dist,
    }


def _sched_compute_group_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    stations = {r.get("statId") for r in rows if r.get("statId")}
    available = sum(1 for r in rows if r.get("stat") == "2")
    charging = sum(1 for r in rows if r.get("stat") == "3")
    fault = sum(1 for r in rows if r.get("stat") in _SCHED_FAULT)
    outputs = [r.get("outputVal", 0.0) for r in rows if (r.get("outputVal") or 0) > 0]
    rapid = sum(1 for o in outputs if 50 <= o < 200)
    slow = sum(1 for o in outputs if o < 50)
    ultra = sum(1 for o in outputs if o >= 200)
    return {
        "stations": len(stations),
        "chargers": total,
        "available": available,
        "charging": charging,
        "fault": fault,
        "availabilityRate": round(available / total * 100, 2) if total else 0,
        "avgOutput": round(sum(outputs) / len(outputs), 2) if outputs else 0,
        "rapidChargers": rapid,
        "slowChargers": slow,
        "ultraFastChargers": ultra,
    }


def _sched_compute_districts(city_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gu_groups: dict[str, list[dict[str, Any]]] = {}
    for row in city_rows:
        gu = row.get("gu")
        if not gu or gu == "구 미상":
            continue
        gu_groups.setdefault(gu, []).append(row)

    result = []
    for gu, rows in gu_groups.items():
        item = _sched_compute_group_stats(rows)
        item["gu"] = gu
        result.append(item)
    return sorted(result, key=lambda x: (-x["chargers"], x["gu"]))


def _sched_compute_dongs(gu_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dong_groups: dict[str, list[dict[str, Any]]] = {}
    for row in gu_rows:
        dong = row.get("dong")
        if not dong or dong == "동 미상":
            continue
        dong_groups.setdefault(dong, []).append(row)

    result = []
    for dong, rows in dong_groups.items():
        item = _sched_compute_group_stats(rows)
        item["dong"] = dong
        result.append(item)
    return sorted(result, key=lambda x: (-x["chargers"], x["dong"]))


def _sched_compute_summary(
    city_rows: list[dict[str, Any]],
    generated_at: datetime,
    generated_at_kst: str,
    bucket: datetime,
) -> dict[str, Any]:
    total = len(city_rows)
    status_counts: dict[str, int] = {}
    for row in city_rows:
        stat = row.get("stat") or "None"
        status_counts[stat] = status_counts.get(stat, 0) + 1
    available = status_counts.get("2", 0)
    charging = status_counts.get("3", 0)
    fault = sum(status_counts.get(s, 0) for s in _SCHED_FAULT)
    unknown = sum(status_counts.get(s, 0) for s in ("0", "9", "None"))
    stations = len({r.get("statId") for r in city_rows if r.get("statId")})
    return {
        "totalChargers": total,
        "totalStations": stations,
        "statusCounts": status_counts,
        "availableCount": available,
        "chargingCount": charging,
        "faultCount": fault,
        "unknownCount": unknown,
        "availabilityRate": round(available / total * 100, 2) if total else 0,
        "generatedAt": generated_at.isoformat(),
        "generatedAtKst": generated_at_kst,
        "collectedAtBucket": bucket.isoformat(),
    }


def generate_precomputed_stats(
    db: Database,
    collected_at: datetime,
    collected_at_bucket: datetime,
) -> None:
    """
    charger_master + charger_current에서 통계를 사전 계산하여
    charger_stats_cache 컬렉션에 bulk upsert합니다.
    수집 메인 플로우가 실패하지 않도록 내부 예외를 전부 흡수합니다.
    """
    logger.info("precomputed stats started")
    try:
        generated_at = utc_now()
        generated_at_kst = to_kst_string(generated_at)
        ref_kst = collected_at.astimezone(KST)
        updated_at_kst = to_kst_string(collected_at)

        # ── 1. charger_current 상태 맵 로드 ──────────────────────────────
        status_map: dict[tuple[str, str], dict[str, Any]] = {}
        for doc in db.charger_current.find(
            {},
            {"_id": 0, "statId": 1, "chgerId": 1, "stat": 1, "statUpdDt": 1, "raw": 1},
        ):
            sid = clean_str(doc.get("statId"))
            cid = clean_str(doc.get("chgerId"))
            if sid and cid:
                status_map[(sid, cid)] = doc

        # ── 2. charger_master 전체 로드 및 행 구성 ───────────────────────
        city_rows: list[dict[str, Any]] = []
        for master in db.charger_master.find(
            {"delYn": {"$ne": "Y"}},
            {
                "_id": 0, "statId": 1, "chgerId": 1, "statNm": 1,
                "addr": 1, "lat": 1, "lng": 1, "chgerType": 1, "output": 1,
                "kind": 1, "kindDetail": 1,
            },
        ):
            sid = clean_str(master.get("statId"))
            cid = clean_str(master.get("chgerId"))
            status = status_map.get((sid, cid), {}) if sid and cid else {}
            raw_status = status.get("raw") or {}
            stat = clean_str(status.get("stat")) or clean_str(raw_status.get("stat"))
            chger_type = clean_str(master.get("chgerType"))
            output_str = master.get("output")
            out_val = to_float(output_str) or 0.0
            addr = master.get("addr")
            city_rows.append({
                "statId": sid,
                "chgerId": cid,
                "statNm": master.get("statNm"),
                "addr": addr,
                "gu": _sched_extract_gu(addr),
                "dong": _sched_extract_dong(addr),
                "lat": master.get("lat"),
                "lng": master.get("lng"),
                "chgerType": chger_type,
                "output": output_str,
                "outputVal": out_val,
                "speed": _sched_speed(chger_type, output_str),
                "kind": master.get("kind"),
                "kindDetail": master.get("kindDetail"),
                "stat": stat,
                "rawStatus": raw_status,
            })

        # ── 3. 캐시 문서 생성 ─────────────────────────────────────────────
        def _make_doc(type_: str, scope_key: str, scope: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
            return {
                "type": type_,
                "scopeKey": scope_key,
                "scope": scope,
                "payload": payload,
                "generatedAt": generated_at,
                "generatedAtKst": generated_at_kst,
                "sourceBucket": collected_at_bucket,
            }

        docs: list[dict[str, Any]] = []

        # city summary
        docs.append(_make_doc(
            "summary", "city", {"level": "city"},
            _sched_compute_summary(city_rows, generated_at, generated_at_kst, collected_at_bucket),
        ))

        # city overview
        docs.append(_make_doc(
            "overview", "city", {"level": "city", "gu": None, "dong": None},
            _sched_compute_overview(city_rows, ref_kst, updated_at_kst),
        ))

        # districts list
        docs.append(_make_doc(
            "districts", "city", {"level": "list"},
            {"items": _sched_compute_districts(city_rows)},
        ))

        # gu groups: per-gu overview + dongs
        gu_groups: dict[str, list[dict[str, Any]]] = {}
        for row in city_rows:
            gu = row.get("gu")
            if gu and gu != "구 미상":
                gu_groups.setdefault(gu, []).append(row)

        for gu, gu_rows in gu_groups.items():
            docs.append(_make_doc(
                "overview", f"gu:{gu}",
                {"level": "gu", "gu": gu, "dong": None},
                _sched_compute_overview(gu_rows, ref_kst, updated_at_kst, gu=gu),
            ))
            docs.append(_make_doc(
                "dongs", f"gu:{gu}",
                {"level": "gu_dongs", "gu": gu},
                {"items": _sched_compute_dongs(gu_rows)},
            ))

        # ── 4. Bulk upsert ────────────────────────────────────────────────
        ops: list[UpdateOne] = [
            UpdateOne(
                {"type": doc["type"], "scopeKey": doc["scopeKey"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]
        if ops:
            db.charger_stats_cache.bulk_write(ops, ordered=False)
        logger.info("precomputed stats saved %d docs", len(docs))

    except Exception:
        logger.exception("precomputed stats failed")


def generate_basic_stats(db: Database, collected_at: datetime | None = None) -> dict[str, Any]:
    """
    charger_status_snapshot에서 간단한 통계를 생성하여 charger_stats에 저장합니다.

    현재는 전체/상태별/zcode별 개수를 저장합니다.
    향후 구/동 단위 통계를 추가할 경우, 이 함수 안에 aggregation pipeline을 추가하면 됩니다.
    """
    config = require_config()

    if collected_at is None:
        latest = db.charger_status_snapshot.find_one(sort=[("collectedAt", -1)])
        if not latest:
            logger.warning("상태 스냅샷이 없어 통계를 생성할 수 없습니다.")
            return {"created": False, "reason": "no_status_snapshot"}
        collected_at = latest["collectedAt"]
        if collected_at.tzinfo is None:
            collected_at = collected_at.replace(tzinfo=timezone.utc)

    collected_at_bucket = floor_datetime_to_minutes(collected_at, config.scheduler_interval_minutes)
    generated_at = utc_now()

    match_filter = {"collectedAtBucket": collected_at_bucket}

    total_count = db.charger_status_snapshot.count_documents(match_filter)

    status_counts_pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$stat", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    zcode_counts_pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$zcode", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]

    status_counts = {
        str(row["_id"]): row["count"]
        for row in db.charger_status_snapshot.aggregate(status_counts_pipeline)
    }
    zcode_counts = {
        str(row["_id"]): row["count"]
        for row in db.charger_status_snapshot.aggregate(zcode_counts_pipeline)
    }

    stats_doc = {
        "type": "basic_status_snapshot",
        "scope": "seoul",
        "zcode": config.seoul_zcode,
        "level": "city",
        "collectedAtBucket": collected_at_bucket,
        "sourcePeriodMinutes": config.status_period_minutes,
        "totalChargers": total_count,
        "statusCounts": status_counts,
        "zcodeCounts": zcode_counts,
        "generatedAt": generated_at,
        "generatedAtKst": to_kst_string(generated_at),
        # 향후 구/동 통계를 넣기 위한 자리입니다.
        # 예: districtStats: [{sido: "서울", sigungu: "강남구", dong: "역삼동", ...}]
        "districtStats": [],
    }

    db.charger_stats.update_one(
        {"type": "basic_status_snapshot", "collectedAtBucket": collected_at_bucket},
        {"$set": stats_doc},
        upsert=True,
    )

    result = {
        "created": True,
        "bucket": collected_at_bucket.isoformat(),
        "totalChargers": total_count,
        "statusCounts": status_counts,
        "zcodeCounts": zcode_counts,
    }
    logger.info("기본 통계 생성 완료: %s", result)
    return result




def generate_current_stats(db: Database, collected_at: datetime | None = None) -> dict[str, Any]:
    """
    charger_current 기준으로 '현재 전체 충전기 상태' 통계를 생성합니다.

    기존 generate_basic_stats는 해당 bucket에 API가 반환한 갱신 데이터 기준입니다.
    대시보드의 전체 현황판은 이 함수의 결과를 사용하는 것이 안전합니다.
    """
    config = require_config()

    if collected_at is None:
        collected_at = utc_now()
    if collected_at.tzinfo is None:
        collected_at = collected_at.replace(tzinfo=timezone.utc)

    collected_at_bucket = floor_datetime_to_minutes(collected_at, config.scheduler_interval_minutes)
    generated_at = utc_now()

    match_filter = {"zcode": config.seoul_zcode}
    total_count = db.charger_current.count_documents(match_filter)

    status_counts_pipeline = [
        {"$match": match_filter},
        {"$group": {"_id": "$stat", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    status_counts = {
        str(row["_id"]): row["count"]
        for row in db.charger_current.aggregate(status_counts_pipeline)
    }

    stats_doc = {
        "type": "current_status",
        "scope": "seoul",
        "zcode": config.seoul_zcode,
        "level": "city",
        "collectedAtBucket": collected_at_bucket,
        "totalChargers": total_count,
        "statusCounts": status_counts,
        "generatedAt": generated_at,
        "generatedAtKst": to_kst_string(generated_at),
    }

    db.charger_stats.update_one(
        {"type": "current_status", "collectedAtBucket": collected_at_bucket},
        {"$set": stats_doc},
        upsert=True,
    )

    result = {
        "created": True,
        "bucket": collected_at_bucket.isoformat(),
        "totalChargers": total_count,
        "statusCounts": status_counts,
    }
    logger.info("현재상태 통계 생성 완료: %s", result)
    return result


# -----------------------------------------------------------------------------
# 스케줄러
# -----------------------------------------------------------------------------


def run_scheduler() -> None:
    """10분 간격으로 상태정보를 수집하는 스케줄러를 실행합니다."""
    config = require_config()
    client = get_mongo_client()
    db = client[config.mongodb_db_name]
    create_indexes(db)

    if config.refresh_master_on_start:
        collect_master(db)
    else:
        logger.info("REFRESH_MASTER_ON_START=false 이므로 시작 시 기본정보 수집은 건너뜁니다.")

    scheduler = BlockingScheduler(timezone=KST)

    # max_instances=1: 이전 수집이 아직 끝나지 않았는데 다음 수집이 겹쳐 실행되는 것을 방지합니다.
    # coalesce=True: 서버가 잠깐 멈췄다가 살아났을 때 밀린 작업을 한꺼번에 실행하지 않습니다.
    scheduler.add_job(
        lambda: collect_status_once(db),
        trigger="interval",
        minutes=config.scheduler_interval_minutes,
        id="collect_status_once",
        name="Collect EV charger status every interval",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(KST),  # 실행하자마자 1회 수집 후, 이후 10분마다 수집합니다.
    )

    if config.master_daily_refresh_enabled:
        scheduler.add_job(
            lambda: collect_master(db),
            trigger="cron",
            hour=config.master_refresh_hour_kst,
            minute=0,
            id="collect_master_daily",
            name="Refresh EV charger master daily",
            max_instances=1,
            coalesce=True,
        )
        logger.info("기본정보 일 1회 갱신 예약: 매일 KST %02d:00", config.master_refresh_hour_kst)

    logger.info("스케줄러 시작: interval=%s분", config.scheduler_interval_minutes)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("스케줄러 종료 요청을 받았습니다.")
    finally:
        client.close()
        logger.info("MongoDB 연결 종료")


# -----------------------------------------------------------------------------
# CLI 진입점
# -----------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="서울 전기차 충전소 API -> MongoDB 수집기")
    parser.add_argument(
        "command",
        choices=["collect-master", "collect-status-once", "generate-stats", "generate-precomputed-stats", "run"],
        help="실행할 명령어",
    )
    args = parser.parse_args()

    try:
        config = load_config()
        client = get_mongo_client()
        db = client[config.mongodb_db_name]
        create_indexes(db)

        if args.command == "collect-master":
            collect_master(db)
        elif args.command == "collect-status-once":
            collect_status_once(db)
        elif args.command == "generate-stats":
            # 기존 bucket 기준 갱신 데이터 통계와 charger_current 기준 전체 현재상태 통계를 함께 생성합니다.
            generate_basic_stats(db)
            generate_current_stats(db)
        elif args.command == "generate-precomputed-stats":
            now = utc_now()
            config = require_config()
            bucket = floor_datetime_to_minutes(now, config.scheduler_interval_minutes)
            generate_precomputed_stats(db, now, bucket)
        elif args.command == "run":
            client.close()
            run_scheduler()
            return
        else:
            parser.print_help()

        client.close()

    except Exception as exc:
        logger.exception("프로그램 실행 실패: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
