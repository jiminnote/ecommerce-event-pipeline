# 이커머스 행동 로그 파이프라인

이커머스 사용자 행동 이벤트를 **정의→생성→수집→검증→적재→마트 생성→모니터링**까지 End-to-End로 처리하는 데이터 파이프라인입니다.
[https://jiminnote.github.io/ecommerce-event-pipeline/](https://jiminnote.github.io/ecommerce-event-pipeline/)
## 프로젝트 목적

- 사용자 행동 데이터(page_view, click, add_to_cart, purchase, search)의 **이벤트 스키마 설계**
- Airflow 기반 **데이터 파이프라인 구축** (수집→검증→분기→적재→마트)
- **7가지 자동화된 데이터 품질 검증** 체계 구현
- 전환 퍼널 분석용 **데이터 마트 4종** 설계
- **Slack Webhook 기반 실시간 알림** + 품질 대시보드 모니터링
- **PySpark 배치 프로세서**를 통한 대용량 처리 확장 설계

## 아키텍처

```
                        Airflow DAG (일 1회, KST 02:00)
                        ================================

  [이벤트 생성기]                              ┌─ mart_user_daily
       │                                       ├─ mart_funnel_daily
       ▼                                       ├─ mart_product_daily
    JSONL 파일                                  └─ mart_orders
       │                                            ▲ (4종 병렬)
       ▼                                            │
  [품질 검증 7종] ──── PASS ──→ [PostgreSQL 적재] ──→ [마트 생성] → [리포트] → [Slack 완료 알림]
       │                                                              ▲
       └──────────── FAIL ──→ [Slack 실패 알림] ─────────────────────→┘
                                                         ↓
                                               [품질 대시보드 HTML]
```

### 대용량 확장 (PySpark)

```
  JSONL 이벤트 로그 (대용량)
       │
       ▼
  [SparkBatchProcessor] ──→ 퍼널 전환율 / 시간대 트래픽 / 상품 전환율 / 세션 시퀀스
       │
       ▼
  Parquet + CSV 출력
```

## 이벤트 스키마 (JSON Schema 표준)

| 이벤트 | 설명 | 필수 필드 |
|--------|------|----------|
| `page_view` | 페이지 조회 | page_url, page_type |
| `click` | 요소 클릭 | element_id, element_type |
| `add_to_cart` | 장바구니 담기 | product_id, quantity, unit_price |
| `purchase` | 주문 완료 | order_id, total_amount, payment_method |
| `search` | 검색 | search_query, result_count |

공통 필드: `event_id`, `user_id`, `session_id`, `timestamp`, `platform`, `device_type`, `os`, `browser`

> 스키마는 [JSON Schema Draft-07](schemas/event_schema.json) 표준을 따르며, 필드별 validation 규칙(pattern, enum, min/max)이 포함되어 있습니다.

## 데이터 품질 검증 (7가지)

| # | 검증 항목 | 유형 | 설명 |
|---|----------|------|------|
| 1 | 필수 필드 NULL 검사 | null_check | 이벤트 유형별 필수 필드 누락 탐지 |
| 2 | event_id 중복 검사 | duplicate_check | 이벤트 고유 ID 중복 탐지 |
| 3 | 필드 값 범위 검사 | range_check | 수량·금액·이벤트 유형 유효성 검증 |
| 4 | 전환 퍼널 순서 검증 | sequence_check | purchase 전 add_to_cart 존재 여부 등 |
| 5 | 타임스탬프 유효성 | range_check | ISO 8601 형식 및 연도 범위 검증 |
| 6 | 주문 금액 정합성 | integrity_check | total_amount와 상품 합계 일치 검증 |
| 7 | 플랫폼-디바이스 정합성 | consistency_check | iOS→mobile, 세션 내 플랫폼 변경 불가 |

검증 기준: 통과율 99% 이상 → PASS, 미만 → FAIL (파이프라인 중단 + Slack 알림 + DB 로깅)

## 모니터링 / 알림

### Slack Webhook 알림

파이프라인 전 구간에 Slack 알림이 연동되어 있습니다.

| 트리거 | 알림 내용 |
|--------|----------|
| Task 실패 | DAG/Task/날짜/에러 메시지 |
| 품질 검증 FAIL | 실패 항목 상세 (항목명, 통과율, 사유) |
| 파이프라인 완료 | 이벤트 수, 사용자 수, 품질 통과율 요약 |

설정: 환경변수 `SLACK_WEBHOOK_URL`에 Incoming Webhook URL을 지정하면 활성화됩니다.
미설정 시 로그 출력으로 fallback되므로 로컬 개발 환경에서도 정상 동작합니다.

### 품질 대시보드

`quality_dashboard.py`로 품질 트렌드를 터미널 리포트 + HTML 대시보드로 확인할 수 있습니다.

```bash
# 로컬 리포트 JSON 기반 대시보드 생성
make dashboard

# PostgreSQL 품질 로그 기반 (Docker 환경)
python scripts/quality_dashboard.py --source db --days 30
```

대시보드 포함 항목:
- 일별 PASS/FAIL 현황, 가용률
- 검증 항목별 실패 빈도
- 이벤트 수, 매출 추이
- HTML 파일로 저장되어 브라우저에서 즉시 확인 가능

## PySpark 배치 프로세서

대용량 이벤트 로그를 Spark로 집계하는 배치 프로세서입니다.
일별 수십만 건 이상의 이벤트를 처리할 때 PostgreSQL 마트 대신 Spark 파이프라인으로 전환할 수 있습니다.

```bash
# 단일 날짜 집계
make spark-batch

# 기간 지정 배치 (spark-submit 직접)
spark-submit scripts/spark_batch_processor.py \
  --input data/events/ \
  --output data/spark_output \
  --start-date 2026-01-01 --end-date 2026-01-31
```

집계 항목 (4종):
| 집계 | 설명 |
|------|------|
| `funnel_conversion` | 플랫폼별 퍼널 전환율 (조회→클릭→장바구니→구매) |
| `hourly_traffic` | 시간대별 이벤트 분포 + 고유 사용자 수 |
| `product_conversion` | 상품별 조회→장바구니→구매 전환율 (JSONB 언네스트) |
| `session_sequences` | 세션 내 이벤트 시퀀스 (Window 함수) |

출력: Parquet (분석용) + CSV (확인용) 이중 저장

## 데이터 마트 (4종, 병렬 생성)

| 마트 | 설명 | 적재 방식 |
|------|------|-----------|
| `mart_user_daily` | 사용자별 일별 행동 요약 (세션수, PV, 클릭, 구매, 매출) | DELETE+INSERT (멱등) |
| `mart_funnel_daily` | 플랫폼별 전환 퍼널 (조회→클릭→장바구니→구매 전환율) | DELETE+INSERT (멱등) |
| `mart_product_daily` | 상품별 행동 집계 (JSONB에서 구매 상품 언네스트) | DELETE+INSERT (멱등) |
| `mart_orders` | 주문 단위 상세 (금액, 결제수단, 할인, 배송비, 상품수) | DELETE+INSERT (멱등) |

> 마트 적재는 `DELETE WHERE date = '{{ ds }}' → INSERT` 패턴으로 **멱등성(idempotency)**을 보장합니다. 재실행 시 중복 없이 동일 결과를 보장합니다.

## 테스트

```bash
# 전체 테스트 실행
make test

# 커버리지 포함
make test-cov
```

**테스트 구성 (30+ 케이스)**:
- `test_generate_events.py`: 이벤트 구조, 플랫폼-디바이스 정합성, 전환 퍼널 순서, 시간대 분포, 상품 카탈로그 일치, 주말 트래픽 패턴
- `test_validate_quality.py`: 7가지 검증 항목별 정상/비정상 케이스, 통합 검증, JSONL 파일 기반 검증

## 주요 설계 결정

| 결정 | 이유 |
|------|------|
| 사용자별 고정 프로파일 | iOS 사용자가 갑자기 desktop으로 잡히면 데이터 신뢰도 하락. 현실 서비스와 동일하게 사용자-디바이스 일관성 보장 |
| 요일별 트래픽 가중치 | 이커머스 특성상 주말 트래픽 30% 증가. 현실적인 트래픽 패턴 시뮬레이션 |
| 시간대별 접속 분포 | 출퇴근(8-9시), 점심(12-13시), 저녁(19-21시) 피크 반영 |
| 마트 SQL 분리 (4파일) | 병렬 실행으로 처리 시간 단축 + 개별 마트 독립 배포 가능 |
| DELETE+INSERT 패턴 | UPSERT 대비 단순하고, 컬럼 추가 시 변경 범위 최소화. 일별 파티션 단위 멱등 보장 |
| JSONB에서 상품 언네스트 | purchase 이벤트의 상품 목록을 정규화하여 상품 마트 정확도 확보 |
| 품질 검증 분기 처리 | FAIL 시 적재를 차단하여 잘못된 데이터가 마트에 반영되는 것을 방지 |
| 품질 로그 DB 저장 | 검증 이력 추적 가능, 시간에 따른 데이터 품질 트렌드 모니터링 |

## 실행 방법

```bash
# 1. Docker Compose로 환경 구축 (Airflow + PostgreSQL + ecommerce DB)
make setup

# 2. Airflow UI 접속
open http://localhost:8080  # admin / admin

# 3. 수동 이벤트 생성 + 검증 (테스트용)
make generate
make validate

# 4. 30일치 데이터 일괄 생성
make generate-month

# 5. 품질 대시보드 생성
make dashboard

# 6. Spark 배치 집계
make spark-batch

# 7. 테스트 실행
make test

# 8. 환경 종료
make down
```

### Slack 알림 설정 (선택)

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"
```

## 프로젝트 구조

```
ecommerce-event-pipeline/
├── dags/
│   └── event_pipeline_dag.py       # Airflow DAG (10개 Task, 품질 분기, 4종 마트 병렬, Slack 연동)
├── scripts/
│   ├── generate_events.py          # 이벤트 로그 생성기 (퍼널 시뮬레이션, 사용자 프로파일)
│   ├── validate_quality.py         # 데이터 품질 검증 모듈 (7가지 검증)
│   ├── spark_batch_processor.py    # PySpark 대용량 배치 프로세서 (4종 집계)
│   ├── slack_alert.py              # Slack Incoming Webhook 알림 모듈
│   └── quality_dashboard.py        # 품질 대시보드 (터미널 + HTML)
├── schemas/
│   └── event_schema.json           # 이벤트 스키마 (JSON Schema Draft-07)
├── sql/
│   ├── create_tables.sql           # RAW + 마트 + 품질로그 + 파이프라인로그 DDL
│   ├── create_marts.sql            # 마트 참조 문서
│   └── marts/
│       ├── mart_user_daily.sql     # 사용자 마트 증분 적재
│       ├── mart_funnel_daily.sql   # 퍼널 마트 증분 적재
│       ├── mart_product_daily.sql  # 상품 마트 (JSONB 언네스트)
│       └── mart_orders.sql         # 주문 마트 증분 적재
├── tests/
│   ├── conftest.py                 # 테스트 fixture
│   ├── test_generate_events.py     # 생성기 테스트 (20+ 케이스)
│   └── test_validate_quality.py    # 검증기 테스트 (20+ 케이스)
├── docker/
│   └── init-db.sh                  # PostgreSQL 초기화 (ecommerce DB 생성)
├── docker-compose.yml              # Airflow + PostgreSQL 환경
├── Makefile                        # 편의 명령어
├── requirements.txt
└── README.md
```

## 기술 스택

| 영역 | 기술 |
|------|------|
| 오케스트레이션 | Apache Airflow 2.7 (BranchPythonOperator, PostgresOperator, TriggerRule) |
| 데이터베이스 | PostgreSQL 15 (JSONB, CTE, Window Functions) |
| 배치 처리 | PySpark 3.5 (DataFrame API, from_json, explode, Window) |
| 알림/모니터링 | Slack Incoming Webhook + 자체 품질 대시보드 |
| 언어 | Python 3.11, SQL |
| 테스트 | pytest, pytest-cov |
| 인프라 | Docker Compose (멀티 서비스, 헬스체크, 볼륨) |
| 품질 관리 | 자체 구축 7종 검증 프레임워크 + 품질 로그 DB |
