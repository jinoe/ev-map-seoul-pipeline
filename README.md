# 서울 전기차 충전소 MongoDB 수집기

`builder.py` 같은 복잡한 프로젝트 구조 없이 `scheduler.py` 하나로 실행하는 프로토타입 수집기입니다.

## 1. 설치

```bash
python -m venv .venv
source .venv/bin/activate  # Windows는 .venv\Scripts\activate
pip install -r requirements.txt
```

## 2. 환경변수 설정

```bash
cp .env.example .env
nano .env
```

`.env`에 아래 값을 넣습니다.

```env
DATA_GO_KR_SERVICE_KEY=공공데이터포털_서비스키
MONGODB_URI=mongodb://user:password@서버IP:27017/ev_charger?authSource=admin
MONGODB_DB_NAME=ev_charger
SEOUL_ZCODE=11
NUM_OF_ROWS=9999
STATUS_PERIOD_MINUTES=10
SCHEDULER_INTERVAL_MINUTES=10
REFRESH_MASTER_ON_START=false
```

> 공공데이터포털 서비스키는 가능하면 Decoding 키를 넣는 것을 권장합니다.

## 3. 기본정보 1회 수집

```bash
python scheduler.py collect-master
```

저장 컬렉션: `charger_master`

## 4. 상태정보 1회 수집

```bash
python scheduler.py collect-status-once
```

저장 컬렉션: `charger_status_snapshot`

상태정보 수집 후 같은 10분 bucket 기준으로 `charger_stats` 기본 통계도 자동 생성합니다.

## 5. 기본 통계 1회 생성

```bash
python scheduler.py generate-stats
```

가장 최근 상태 스냅샷 bucket 기준으로 통계를 다시 생성합니다.

## 6. 10분 간격 스케줄러 실행

```bash
python scheduler.py run
```

동작 순서:

1. 환경변수 로드
2. MongoDB 연결
3. 인덱스 생성
4. `REFRESH_MASTER_ON_START=true`이면 기본정보 1회 수집
5. 상태정보를 즉시 1회 수집
6. 이후 10분마다 상태정보 수집
7. 상태 수집이 끝날 때마다 `charger_stats` 생성

## 7. MongoDB 컬렉션 확인

서버에서 `mongosh` 접속 후 확인합니다.

```javascript
use ev_charger
show collections

db.charger_master.countDocuments()
db.charger_status_snapshot.countDocuments()
db.charger_stats.find().sort({ generatedAt: -1 }).limit(3).pretty()

db.charger_status_snapshot.find().sort({ collectedAt: -1 }).limit(3).pretty()
```

## 8. 단일 파일 구조가 현재 적절한 이유

현재 단계는 API 연결, 페이지네이션, MongoDB 저장, 10분 주기 실행이 핵심인 프로토타입입니다. 이 단계에서 파일을 여러 개로 나누면 구조 이해 비용이 커지고, 서버에 올릴 때도 관리 포인트가 늘어납니다. 따라서 먼저 `scheduler.py` 하나로 안정적으로 수집이 되는지 검증한 뒤, 데이터가 쌓이고 API/통계 요구사항이 늘어나는 시점에 `config`, `api`, `repository`, `service`, `scheduler` 등으로 분리하는 편이 좋습니다.

## 9. 운영 참고

터미널을 닫아도 계속 실행하려면 서버에서는 보통 `tmux`, `screen`, `nohup`, 또는 `systemd`를 사용합니다.

예시:

```bash
nohup python scheduler.py run > scheduler.log 2>&1 &
tail -f scheduler.log
```
