# 이커머스 행동 로그 파이프라인

사용자 행동 이벤트(page_view, click, add_to_cart, purchase, search)를 수집→검증→적재→마트 생성까지 처리하는 End-to-End 데이터 파이프라인입니다.

> Airflow 오케스트레이션 / PostgreSQL 적재 / 7가지 품질 검증 / 전환 퍼널 마트 4종 / Slack 알림

## 아키텍처

```
이벤트 생성 → JSONL 수집 → 품질 검증 (7종) ─── PASS → RAW 적재 → 마트 4종 (병렬) → Slack 완료 알림
                                         └── FAIL → 파이프라인 중단 + Slack 경고
```

## 이벤트 스키마

[JSON Schema Draft-07](schemas/event_schema.json) 표준 기반으로 5가지 이벤트를 정의했습니다.

| 이벤트 | 설명 | 주요 필드 |
|--------|------|----------|
| `page_view` | 페이지 조회 | page_url, page_type |
| `click` | 요소 클릭 | element_id, element_type |
| `add_to_cart` | 장바구니 담기 | product_id, quantity, unit_price |
| `purchase` | 주문 완료 | order_id, total_amount, payment_method |
| `search` | 검색 | search_query, result_count |

공통 필드: `event_id`, `user_id`, `session_id`, `timestamp`, `platform`, `device_type`

## 데이터 품질 검증

파이프라인 중간에 7가지 검증을 수행하고, 통과율 99% 미만 시 적재를 차단합니다.

| # | 검증 항목 | 설명 |
|---|----------|------|
| 1 | 필수 필드 NULL 검사 | 이벤트 유형별 필수 필드 누락 탐지 |
| 2 | event_id 중복 검사 | 이벤트 고유 ID 중복 탐지 |
| 3 | 필드 값 범위 검사 | 수량·금액·이벤트 유형 유효성 |
| 4 | 전환 퍼널 순서 검증 | purchase 전 add_to_cart 존재 여부 |
| 5 | 타임스탬프 유효성 | ISO 8601 형식 및 연도 범위 |
| 6 | 주문 금액 정합성 | total_amount와 상품 합계 일치 |
| 7 | 플랫폼-디바이스 정합성 | iOS→mobile, 세션 내 플랫폼 변경 불가 |

> FAIL 시 Slack 알림 + 품질 로그 DB 저장으로 이력을 추적합니다.

## 데이터 마트 (4종)

| 마트 | 설명 |
|------|------|
| `mart_user_daily` | 사용자별 일별 행동 요약 (세션수, PV, 클릭, 구매, 매출) |
| `mart_funnel_daily` | 플랫폼별 전환 퍼널 (조회→클릭→장바구니→구매 전환율) |
| `mart_product_daily` | 상품별 행동 집계 (JSONB에서 구매 상품 언네스트) |
| `mart_orders` | 주문 단위 상세 (금액, 결제수단, 할인, 배송비) |

적재 방식: `DELETE WHERE date = '{{ ds }}' → INSERT` 패턴으로 멱등성을 보장합니다.

## 주요 설계 결정

| 결정 | 이유 |
|------|------|
| 사용자별 고정 프로파일 | iOS 사용자가 갑자기 desktop으로 잡히면 데이터 신뢰도 하락. 사용자-디바이스 일관성 보장 |
| 품질 검증 FAIL 시 적재 차단 | 잘못된 데이터가 마트에 반영되는 것을 방지 (BranchPythonOperator로 분기) |
| DELETE+INSERT 패턴 | UPSERT 대비 단순하고, 컬럼 추가 시 변경 범위 최소화. 일별 파티션 단위 멱등 보장 |
| 마트 SQL 4파일 분리 | 병렬 실행으로 처리 시간 단축 + 개별 마트 독립 배포 가능 |
| JSONB에서 상품 언네스트 | purchase 이벤트의 상품 목록을 정규화하여 상품 마트 정확도 확보 |
| 품질 로그 DB 저장 | 시간에 따른 데이터 품질 트렌드 모니터링 가능 |

## 실행 방법

```bash
make setup          # Docker Compose 환경 구축 (Airflow + PostgreSQL)
make test           # 테스트 실행 (50+ 케이스)

# Airflow UI: http://localhost:8080 (admin / admin)
```

## 기술 스택

| 영역 | 기술 |
|------|------|
| 오케스트레이션 | Apache Airflow 2.7 |
| 데이터베이스 | PostgreSQL 15 (JSONB, CTE, Window Functions) |
| 알림 | Slack Incoming Webhook |
| 테스트 | pytest (50+ 케이스) |
| 인프라 | Docker Compose |
| 언어 | Python 3.11, SQL |
