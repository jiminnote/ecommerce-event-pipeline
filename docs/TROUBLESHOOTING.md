# 트러블슈팅 및 설계 의사결정

> 파이프라인 개발 중 마주친 문제들과 설계 선택의 근거를 기록합니다.
> 향후 유지보수 시 "왜 이렇게 했지?" 라는 질문에 대한 답변 문서입니다.

---

## 목차

1. [마트 적재: DELETE+INSERT vs UPSERT 선택 근거](#1-마트-적재-deleteinsert-vs-upsert-선택-근거)
2. [JSONB 언네스트 시 NULL 처리](#2-jsonb-언네스트-시-null-처리)
3. [마트 병렬 실행 시 Lock 이슈](#3-마트-병렬-실행-시-lock-이슈)
4. [전환율 계산 시 Division by Zero](#4-전환율-계산-시-division-by-zero)
5. [Batch Insert 시 메모리 관리](#5-batch-insert-시-메모리-관리)
6. [품질 검증 임계값 99% 선택 근거](#6-품질-검증-임계값-99-선택-근거)
7. [DAG retry 지수 백오프 설정](#7-dag-retry-지수-백오프-설정)
8. [타임존 처리 (KST vs UTC)](#8-타임존-처리-kst-vs-utc)
9. [XCom을 통한 Task 간 데이터 전달 한계](#9-xcom을-통한-task-간-데이터-전달-한계)
10. [BranchPythonOperator 이후 trigger_rule 설정](#10-branchpythonoperator-이후-trigger_rule-설정)

---

## 1. 마트 적재: DELETE+INSERT vs UPSERT 선택 근거

### 문제

마트 테이블에 일별 집계 데이터를 적재할 때, 같은 날짜를 재실행하면 데이터가 중복 적재됩니다.

### 선택지

| 방식 | SQL 패턴 | 장점 | 단점 |
|------|----------|------|------|
| **DELETE+INSERT** | `DELETE WHERE date = '{{ ds }}'` → `INSERT` | 멱등성 보장, 전체 재계산으로 정합성 확보 | 대량 데이터 시 DELETE 비용 |
| **UPSERT** | `INSERT ... ON CONFLICT DO UPDATE` | 변경분만 업데이트, 효율적 | 복합 PK 설계 필수, 집계 컬럼 업데이트 복잡 |
| **TRUNCATE+INSERT** | `TRUNCATE` → `INSERT` | 가장 단순 | 전체 테이블 재적재, 비현실적 |

### 결정: DELETE+INSERT

```sql
-- sql/marts/mart_user_daily.sql
DELETE FROM mart_user_daily WHERE activity_date = '{{ ds }}';
INSERT INTO mart_user_daily (...) SELECT ... WHERE DATE(timestamp) = '{{ ds }}';
```

**근거:**
- **멱등성(Idempotency):** Airflow DAG가 재실행되더라도 동일한 결과를 보장합니다. 장애 복구 시 `airflow dags backfill` 으로 특정 날짜만 재처리할 수 있습니다.
- **집계 마트 특성:** UPSERT는 row 단위 업데이트에 적합하지만, 집계 쿼리(`COUNT`, `SUM`, `COUNT DISTINCT`)의 결과를 부분 업데이트하는 것은 논리적으로 불가능합니다. 해당 날짜의 전체 집계를 다시 계산하는 게 정확합니다.
- **성능:** 일별 데이터 규모(수천~수만 건)에서 DELETE 비용은 무시할 수 있는 수준입니다. 파티션 테이블로 전환하면 `TRUNCATE PARTITION`으로 더 빠르게 처리할 수 있습니다.

> **UPSERT가 적합한 경우:** `raw_events` 테이블의 개별 이벤트 적재에서는 `ON CONFLICT (event_id) DO NOTHING`을 사용합니다. 이는 원천 데이터의 행 단위 중복 방지에 적합합니다.

### 향후 개선

데이터 규모가 커지면 날짜별 파티셔닝을 도입하여 `TRUNCATE PARTITION` → `INSERT`로 전환할 수 있습니다.

---

## 2. JSONB 언네스트 시 NULL 처리

### 문제

`raw_events.extra_data`(JSONB)에서 상품 목록을 추출할 때, `extra_data`가 NULL이거나 `products` 키가 없으면 쿼리가 실패하거나 빈 결과를 반환합니다.

```sql
-- ❌ extra_data가 NULL이면 jsonb_array_elements가 에러 발생
SELECT (item->>'product_id') AS product_id
FROM raw_events,
     jsonb_array_elements(extra_data->'products') AS item
```

### 해결

```sql
-- ✅ COALESCE로 NULL 방어 + WHERE 조건으로 필터링
SELECT (item->>'product_id') AS product_id
FROM raw_events,
     jsonb_array_elements(COALESCE(extra_data->'products', '[]'::jsonb)) AS item
WHERE event_type = 'purchase'
  AND extra_data IS NOT NULL
```

**실제 적용 위치:** `sql/marts/mart_product_daily.sql`, `sql/marts/mart_orders.sql`

### 핵심 포인트

| 상황 | `extra_data->'products'` 결과 | `jsonb_array_elements()` 동작 |
|------|-------------------------------|-------------------------------|
| 정상 JSON 배열 | `[{...}, {...}]` | 정상 언네스트 |
| `extra_data` = NULL | NULL | **에러 발생** |
| `products` 키 없음 | NULL | **에러 발생** |
| 빈 배열 | `[]` | 0 rows 반환 (정상) |

`COALESCE(..., '[]'::jsonb)`로 NULL을 빈 배열로 변환하면 `jsonb_array_elements`가 에러 없이 0 rows를 반환합니다.

### JSONB 캐스팅 주의

```sql
-- 문자열 추출 후 타입 캐스팅 시 NULL 방어 필수
COALESCE((extra_data->>'discount_amount')::NUMERIC(12,2), 0) AS discount_amount
COALESCE((extra_data->>'shipping_fee')::NUMERIC(12,2), 0)    AS shipping_fee
```

`->>` 연산자는 문자열을 반환하므로, 값이 없으면 NULL → `::NUMERIC` 캐스팅은 NULL에 대해 NULL을 반환 → `COALESCE`로 0 기본값을 설정합니다.

---

## 3. 마트 병렬 실행 시 Lock 이슈

### 문제

4개 마트(user, funnel, product, order)가 동시에 `raw_events` 테이블을 읽으면서 마트 테이블에 `DELETE+INSERT`를 수행합니다. 동시 실행 시 잠재적 lock 문제가 발생할 수 있습니다.

```
load_to_database → [create_user_mart, create_funnel_mart, create_product_mart, create_order_mart]
                    ↑ 4개 Task가 동시 실행 (Airflow worker)
```

### 분석

| Lock 유형 | 발생 조건 | 우리 케이스 |
|-----------|----------|------------|
| **Table-level lock** | DDL (ALTER, TRUNCATE) | ❌ 사용 안 함 |
| **Row-level exclusive lock** | UPDATE, DELETE | ⚠️ DELETE 시 발생 |
| **Row-level shared lock** | SELECT | ❌ PostgreSQL은 읽기에 lock 없음 |

### 해결: 문제 없음 (단, 조건부)

**안전한 이유:**
- 4개 마트는 **서로 다른 테이블**에 DELETE+INSERT를 수행합니다. (`mart_user_daily`, `mart_funnel_daily`, `mart_product_daily`, `mart_orders`)
- PostgreSQL MVCC에서 `SELECT`(읽기)는 다른 트랜잭션의 `DELETE/INSERT`와 충돌하지 않습니다.
- `raw_events`에 대한 읽기는 모두 `SELECT`이므로 lock 경합이 없습니다.

**주의할 상황:**
```
⚠️ 만약 여러 마트가 같은 테이블에 쓰기를 시도한다면 deadlock 가능
   → 현재 설계에서는 각 마트가 독립된 테이블에 쓰기하므로 안전
```

### 향후 데이터 증가 시 고려사항

| 규모 | 대응 |
|------|------|
| ~100만 건/일 | 현재 구조 유지 (병렬 SELECT 문제없음) |
| 100만~1000만 건 | `raw_events`에 날짜 파티셔닝 도입, 인덱스 활용 확인 |
| 1000만 건 이상 | Spark 배치 처리로 전환, 마트 적재를 Spark → DB 방식으로 변경 |

---

## 4. 전환율 계산 시 Division by Zero

### 문제

퍼널 마트에서 전환율 계산 시, 특정 날짜에 page_view가 0건이면 `0/0` → 에러가 발생합니다.

```sql
-- ❌ step1_viewers가 0이면 division by zero
COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END)::NUMERIC /
COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) * 100
```

### 해결

```sql
-- ✅ NULLIF로 0 → NULL 변환 (NULL 나누기는 NULL 반환, 에러 아님)
ROUND(
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END)::NUMERIC /
    NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END), 0) * 100,
    2
) AS view_to_click_rate
```

**적용 위치:** `sql/marts/mart_funnel_daily.sql`, `sql/marts/mart_product_daily.sql`

### 동작 원리

```
NULLIF(x, 0)
  → x가 0이면 NULL 반환
  → x가 0이 아니면 x 그대로 반환

y / NULL = NULL  (에러가 아님!)
```

결과적으로 분모가 0인 경우 전환율은 `NULL`로 저장되며, 이는 "계산 불가"를 의미합니다. 대시보드에서는 `COALESCE(conversion_rate, 0)` 또는 "N/A"로 표시하면 됩니다.

---

## 5. Batch Insert 시 메모리 관리

### 문제

`load_to_database` Task에서 JSONL 파일을 한 번에 메모리에 올리고 DB에 적재합니다. 이벤트 수가 수십만 건을 넘으면 OOM 위험이 있습니다.

### 해결: 1000건 단위 배치

```python
# dags/event_pipeline_dag.py - load_to_database_task()
batch_size = 1000
conn = hook.get_conn()
cursor = conn.cursor()
for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    cursor.executemany(insert_sql, batch)
conn.commit()
```

### 선택지 비교

| 방식 | 메모리 | 속도 | 복잡도 |
|------|--------|------|--------|
| 1건씩 INSERT | 최소 | 매우 느림 (N회 roundtrip) | 낮음 |
| **배치 executemany** | 중간 | 빠름 | 중간 |
| COPY (psycopg2 copy_from) | 최소 | 가장 빠름 | 높음 |
| executemany + execute_values | 중간 | 더 빠름 | 중간 |

**현재 1000건 배치를 선택한 이유:**
- `executemany`는 psycopg2에서 기본 제공하며 별도 import가 불필요합니다.
- 일 처리량 ~2만 건 기준으로 20회 배치면 충분히 빠릅니다.
- `ON CONFLICT DO NOTHING`과 호환되어 중복 방지가 가능합니다.

### 향후 개선

대량 데이터(10만 건 이상) 처리 시:

```python
# psycopg2.extras.execute_values가 executemany보다 5~10배 빠름
from psycopg2.extras import execute_values
execute_values(cursor, insert_sql_template, rows, page_size=1000)
```

또는 `COPY` 프로토콜로 전환:

```python
from io import StringIO
buf = StringIO()
# CSV 형식으로 버퍼에 쓰고
cursor.copy_from(buf, 'raw_events', sep=',', columns=(...))
```

---

## 6. 품질 검증 임계값 99% 선택 근거

### 문제

품질 검증의 PASS/FAIL 기준을 몇 %로 설정할 것인가?

### 결정: 99.0%

```python
# scripts/validate_quality.py - QualityCheckResult.__post_init__()
self.pass_rate = round((1 - self.failed_records / max(self.total_records, 1)) * 100, 2)
self.status = "PASS" if self.pass_rate >= 99.0 else "FAIL"
```

### 근거

| 임계값 | 특성 |
|--------|------|
| 100% | 1건의 이상도 허용 안 함 → 너무 엄격, 잦은 false alarm |
| **99%** | 1%의 마진 허용 → 실질적 이상 탐지 + 운영 안정성 |
| 95% | 5%의 이상 허용 → 심각한 품질 문제 놓칠 가능성 |

**99%를 선택한 이유:**
- 일 2만 건 기준, 200건까지의 이상치는 허용합니다. (네트워크 지연, 클라이언트 버그 등 현실적 노이즈)
- 1% 이상 실패하면 시스템적 문제(스키마 변경, 생성기 버그 등)일 가능성이 높습니다.
- 업계 표준: 이커머스 이벤트 로그의 일반적 품질 기준은 95~99%이며, 99%는 상위 수준입니다.

### 향후 개선

검증 항목별로 차등 임계값을 적용할 수 있습니다:

```python
THRESHOLDS = {
    "null_check": 99.5,         # 필수 필드 NULL은 엄격
    "duplicate_check": 99.9,    # 중복은 거의 허용 안 함
    "range_check": 98.0,        # 범위 초과는 다소 관대
    "sequence_check": 95.0,     # 퍼널 순서 이상은 관대 (브라우저 뒤로가기 등)
}
```

---

## 7. DAG retry 지수 백오프 설정

### 문제

Task 실패 시 즉시 재시도하면 일시적 장애(DB 연결 끊김, 리소스 부족)가 해소되지 않은 상태에서 반복 실패합니다.

### 해결: 지수 백오프 (Exponential Backoff)

```python
# dags/event_pipeline_dag.py - default_args
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),           # 초기 대기: 2분
    "retry_exponential_backoff": True,              # 지수 백오프 활성화
    "max_retry_delay": timedelta(minutes=30),       # 최대 대기: 30분
}
```

### 실제 재시도 간격

| 시도 | 대기 시간 | 설명 |
|------|----------|------|
| 1차 실패 | ~2분 | base delay |
| 2차 실패 | ~4분 | 2 × base |
| 3차 실패 | ~8분 | 4 × base (최종 실패 → on_failure_callback) |

### Task별 차등 설정

```python
# DB 적재는 가장 실패 가능성이 높음 → 5회 재시도
load_to_database = PythonOperator(
    retries=5,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(minutes=45),
)

# Slack 알림은 재시도 의미 없음 → 1회만
quality_alert = PythonOperator(
    retries=1,
    execution_timeout=timedelta(minutes=5),
)
```

**근거:**
- **DB 적재 (5회):** PostgreSQL 연결 끊김, 디스크 I/O 지연 등 일시적 인프라 장애가 가장 빈번합니다.
- **알림 (1회):** Slack webhook 실패는 재시도해도 보통 해결되지 않습니다. 무한 재시도보다는 로그에 기록하는 게 합리적입니다.
- **마트 생성 (3회):** SQL 쿼리 자체는 멱등하므로 재시도 시 안전합니다.

---

## 8. 타임존 처리 (KST vs UTC)

### 문제

Airflow는 내부적으로 UTC를 사용하지만, 비즈니스 데이터는 KST 기준입니다. `schedule_interval`과 `{{ ds }}` 템플릿 변수의 시간대가 혼동될 수 있습니다.

### 해결

```python
# DAG 스케줄: UTC 17:00 = KST 다음날 02:00
schedule_interval="0 17 * * *"
```

| 시간 | 의미 |
|------|------|
| UTC 17:00 (2026-01-14) | = KST 02:00 (2026-01-15) |
| `{{ ds }}` = `2026-01-14` | Airflow execution_date (논리적 날짜) |
| 처리 대상 데이터 | 2026-01-14의 이벤트 (전일 데이터) |

### 핵심 규칙

- **Airflow `{{ ds }}`는 UTC 기준 실행일자**이며, 이를 그대로 데이터 필터 조건으로 사용합니다.
- 이벤트 생성 시 `target_date`로 `{{ ds }}`를 전달하므로, UTC 날짜와 이벤트 날짜가 일치합니다.
- PostgreSQL `TIMESTAMP` 컬럼은 타임존 정보 없이 저장하며, 모든 시간은 KST 기준으로 통일합니다.

### 주의사항

```
⚠️ execution_date와 실제 실행 시각은 다릅니다!
   
   schedule_interval="0 17 * * *" 설정 시:
   - execution_date = 2026-01-14 (논리적 날짜)
   - 실제 실행 시각 = 2026-01-15 UTC 17:00 (다음 schedule 시점에 실행)
   
   Airflow 2.2+ 에서는 data_interval_start / data_interval_end 사용 권장
```

---

## 9. XCom을 통한 Task 간 데이터 전달 한계

### 문제

이벤트 파일 경로, 품질 검증 리포트 등을 Task 간에 전달해야 합니다. Airflow XCom은 메타데이터 DB에 직렬화하여 저장하므로 대용량 데이터에 부적합합니다.

### 현재 설계: XCom에 경로/요약만 전달

```python
# 파일 경로만 전달 (파일 자체를 XCom에 넣지 않음)
context["ti"].xcom_push(key="events_filepath", value=filepath)

# 요약 정보만 전달 (수 KB 수준)
context["ti"].xcom_push(key="events_summary", value=json.dumps(summary))
context["ti"].xcom_push(key="quality_report", value=json.dumps(report))
```

### XCom에 넣으면 안 되는 것

| 데이터 | 크기 | XCom 적합 | 대안 |
|--------|------|-----------|------|
| 파일 경로 (string) | ~100B | ✅ | - |
| 품질 리포트 (JSON) | ~2KB | ✅ | - |
| 이벤트 요약 (JSON) | ~500B | ✅ | - |
| 이벤트 전체 리스트 | ~5MB+ | ❌ | 파일시스템 또는 S3 |
| DataFrame | ~수십MB | ❌ | 파일시스템 또는 S3 |

### 향후 개선

클라우드 환경에서는 XCom Backend를 S3/GCS로 교체할 수 있습니다:

```python
# airflow.cfg
[core]
xcom_backend = airflow.providers.amazon.aws.xcom_backends.s3.S3XComBackend
```

---

## 10. BranchPythonOperator 이후 trigger_rule 설정

### 문제

품질 검증 결과에 따라 PASS/FAIL 경로를 분기합니다. 분기 후 공통으로 실행되어야 하는 `save_quality_log`와 `quality_report`가 기본 `trigger_rule=all_success`로는 실행되지 않습니다.

```
quality_branch ─┬─ [PASS] load_to_database → [4개 마트] → save_quality_log → quality_report
                └─ [FAIL] quality_alert ─────────────────→ save_quality_log → quality_report
```

### 원인

`BranchPythonOperator`는 선택되지 않은 경로의 Task를 `skipped` 상태로 만듭니다. 기본 `trigger_rule=all_success`는 upstream 중 하나라도 skipped이면 해당 Task도 skip됩니다.

### 해결

```python
save_quality_log = PythonOperator(
    task_id="save_quality_log",
    python_callable=save_quality_log_task,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # ← 핵심
    dag=dag,
)
```

### trigger_rule 비교

| Rule | 조건 | 사용 시나리오 |
|------|------|-------------|
| `all_success` (기본) | 모든 upstream 성공 | 일반적인 순차 실행 |
| `one_success` | upstream 중 1개 이상 성공 | OR 조건 분기 합류 |
| **`none_failed_min_one_success`** | 실패 없음 + 1개 이상 성공 | Branch 후 합류 (skip 허용) |
| `all_done` | 모든 upstream 완료 (성공/실패/skip 무관) | 무조건 실행 |

**`none_failed_min_one_success`를 선택한 이유:**
- PASS 경로 실행 시: 마트 4개 성공 + quality_alert skipped → **실행됨** ✅
- FAIL 경로 실행 시: quality_alert 성공 + 마트 4개 skipped → **실행됨** ✅
- 마트 중 하나가 실패 시: failed 존재 → **실행 안 됨** (의도적, 불완전한 데이터로 로그 남기지 않음)

---

## 부록: 자주 발생하는 운영 이슈

### A. Docker 환경에서 init-db.sh가 실행되지 않음

```bash
# 증상: ecommerce DB가 생성되지 않음
# 원인: PostgreSQL 컨테이너 볼륨에 이미 데이터가 있으면 init 스크립트 무시

# 해결: 볼륨 삭제 후 재시작
docker compose down -v
docker compose up -d
```

### B. Airflow Connection 설정 누락

```bash
# 증상: PostgresHook에서 "connection 'ecommerce_db' is not defined" 에러
# 원인: docker-compose.yml의 환경변수 방식 연결이 로드되지 않음

# 확인:
docker compose exec airflow-webserver airflow connections list | grep ecommerce

# 수동 등록 (필요 시):
docker compose exec airflow-webserver airflow connections add \
    'ecommerce_db' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-schema 'ecommerce'
```

### C. 마트 SQL에서 `{{ ds }}` 템플릿이 렌더링되지 않음

```bash
# 증상: SQL에 '{{ ds }}'가 문자열 그대로 들어감
# 원인: PostgresOperator의 sql 파라미터에 파일 경로를 넘기면
#       Airflow가 Jinja 템플릿으로 렌더링해줌.
#       단, template_searchpath가 올바르게 설정되어야 함.

# 확인:
dag = DAG(
    template_searchpath=[os.path.join(PROJECT_DIR, "sql")],  # ← 필수
)
```

### D. 테스트에서 Airflow import 에러

```bash
# 증상: test 실행 시 "No module named 'airflow'" 에러
# 원인: 테스트는 로컬 환경에서 실행, Airflow는 Docker 내부에서만 설치됨

# 해결: 테스트 대상은 scripts/ 모듈만 (Airflow 의존성 없는 순수 Python)
#       DAG 파일 자체의 테스트는 Docker 환경 내에서 실행
pytest tests/  # scripts/ 모듈 테스트 (로컬)
docker compose exec airflow-webserver airflow dags test ecommerce_event_pipeline 2026-01-15  # DAG 테스트 (Docker)
```
