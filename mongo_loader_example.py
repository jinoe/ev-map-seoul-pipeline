"""
서울 전기차 충전소 데이터 MongoDB 적재 모듈

run_seoul_rowdata_once.py 의 main() 마지막에서 호출하거나
독립적으로 실행 가능한 모듈이다.

사용 예시 (run_seoul_rowdata_once.py 마지막에 추가):
    from mongo_loader import upsert_to_mongo
    upsert_to_mongo(full_snapshot=full_snapshot, collected_at=collected_at)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
import os

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError


# ── 환경변수에서 읽거나 기본값 사용 ──────────────────────────────────────────
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "ev_charging")


def get_client() -> MongoClient:
    return MongoClient(MONGO_URL)


# ── 1. full_snapshot → charging_logs 컬렉션에 적재 ───────────────────────────
def upsert_charging_logs(
    df: pd.DataFrame,
    collected_at: str,
    db_name: str = MONGO_DB,
) -> dict:
    """
    10분마다 수집된 full_snapshot을 charging_logs 컬렉션에 upsert한다.

    중복 기준: statId + chgerId + collected_at
    같은 수집 시각에 같은 충전기가 이미 있으면 덮어쓰고, 없으면 삽입한다.
    """
    if df.empty:
        return {"upserted": 0, "modified": 0}

    records = df.to_dict(orient="records")

    # NaN → None 변환 (MongoDB는 NaN 저장 불가)
    clean_records = []
    for record in records:
        clean = {
            k: (None if isinstance(v, float) and pd.isna(v) else v)
            for k, v in record.items()
        }
        clean["collected_at"] = collected_at
        clean_records.append(clean)

    operations = [
        UpdateOne(
            filter={
                "statId": record.get("statId"),
                "chgerId": record.get("chgerId"),
                "collected_at": record.get("collected_at"),
            },
            update={"$set": record},
            upsert=True,
        )
        for record in clean_records
    ]

    client = get_client()
    try:
        collection = client[db_name]["charging_logs"]
        result = collection.bulk_write(operations, ordered=False)
        return {
            "upserted": result.upserted_count,
            "modified": result.modified_count,
            "total": len(operations),
        }
    except BulkWriteError as e:
        print(f"[mongo_loader] BulkWriteError: {e.details}")
        raise
    finally:
        client.close()


# ── 2. latest_snapshot → stations 컬렉션에 upsert ────────────────────────────
def upsert_stations(
    df: pd.DataFrame,
    db_name: str = MONGO_DB,
) -> dict:
    """
    충전소 기준정보 + 최신 상태를 stations 컬렉션에 upsert한다.

    중복 기준: statId + chgerId
    항상 최신 상태로 덮어쓴다.
    """
    if df.empty:
        return {"upserted": 0, "modified": 0}

    records = df.to_dict(orient="records")

    clean_records = [
        {
            k: (None if isinstance(v, float) and pd.isna(v) else v)
            for k, v in record.items()
        }
        for record in records
    ]

    operations = [
        UpdateOne(
            filter={
                "statId": record.get("statId"),
                "chgerId": record.get("chgerId"),
            },
            update={"$set": record},
            upsert=True,
        )
        for record in clean_records
    ]

    client = get_client()
    try:
        collection = client[db_name]["stations"]
        result = collection.bulk_write(operations, ordered=False)
        return {
            "upserted": result.upserted_count,
            "modified": result.modified_count,
            "total": len(operations),
        }
    except BulkWriteError as e:
        print(f"[mongo_loader] BulkWriteError: {e.details}")
        raise
    finally:
        client.close()


# ── 3. 인덱스 초기 세팅 (최초 1회 실행) ──────────────────────────────────────
def ensure_indexes(db_name: str = MONGO_DB) -> None:
    """
    필수 인덱스를 생성한다. 이미 있으면 무시된다.
    서버 최초 세팅 시 한 번만 실행하면 된다.
    """
    client = get_client()
    try:
        db = client[db_name]

        # charging_logs 인덱스
        db["charging_logs"].create_index([("timestamp", -1)])
        db["charging_logs"].create_index([("statId", 1)])
        db["charging_logs"].create_index([("statId", 1), ("collected_at", -1)])
        db["charging_logs"].create_index(
            [("collected_at", 1)],
            expireAfterSeconds=604800,  # 7일 TTL
            name="ttl_collected_at",
        )

        # stations 인덱스
        db["stations"].create_index(
            [("statId", 1), ("chgerId", 1)],
            unique=True,
            name="unique_station_charger",
        )

        print("[mongo_loader] 인덱스 설정 완료")
    finally:
        client.close()


# ── 4. 통합 호출 함수 (run_seoul_rowdata_once.py에서 이걸 호출하면 됨) ────────
def upsert_to_mongo(
    full_snapshot: pd.DataFrame,
    collected_at: str,
    db_name: str = MONGO_DB,
) -> None:
    """
    run_seoul_rowdata_once.py 의 main() 마지막에서 호출한다.

    full_snapshot 을 받아:
    1. charging_logs 에 이번 수집분 upsert
    2. stations 에 최신 상태 upsert
    """
    try:
        logs_result = upsert_charging_logs(
            df=full_snapshot,
            collected_at=collected_at,
            db_name=db_name,
        )
        print(
            f"[mongo_loader] charging_logs upsert 완료 "
            f"upserted={logs_result['upserted']} "
            f"modified={logs_result['modified']} "
            f"total={logs_result['total']}"
        )

        stations_result = upsert_stations(df=full_snapshot, db_name=db_name)
        print(
            f"[mongo_loader] stations upsert 완료 "
            f"upserted={stations_result['upserted']} "
            f"modified={stations_result['modified']} "
            f"total={stations_result['total']}"
        )

    except Exception as exc:
        print(f"[mongo_loader] MongoDB 적재 실패: {exc}")
        raise


# ── 직접 실행 시 인덱스 세팅 ─────────────────────────────────────────────────
if __name__ == "__main__":
    print("[mongo_loader] 인덱스 초기 세팅 시작")
    ensure_indexes()
