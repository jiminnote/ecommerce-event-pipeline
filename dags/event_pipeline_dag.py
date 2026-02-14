"""
이커머스 행동 로그 파이프라인 DAG
====================================
일별 실행: 이벤트 생성 → 품질 검증 → 분기 → DB 적재 → 마트 생성(병렬) → 리포트

Task 흐름:
  generate_events
      → validate_quality
          → quality_branch
              ├─ [PASS] load_to_database
              │     → [create_user_mart  ┐
              │        create_funnel_mart ├→ save_quality_log → quality_report
              │        create_product_mart│
              │        create_order_mart  ┘]
              └─ [FAIL] quality_alert ──────→ save_quality_log → quality_report

스케줄: 매일 02:00 KST (전일 데이터 처리)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

import json
import logging
import os
import sys

logger = logging.getLogger(__name__)

# 프로젝트 경로 추가
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)


# ========== 모니터링 콜백 ==========

def on_failure_callback(context):
    """Task 실패 시 Slack 알림 (retry 소진 후 최종 실패 시에만 발송)"""
    from scripts.slack_alert import SlackAlert

    ti = context.get("task_instance")
    task_id = ti.task_id
    dag_id = context.get("dag").dag_id
    exec_date = context.get("ds")
    error = context.get("exception")
    try_number = ti.try_number
    max_tries = ti.max_tries

    alert_msg = (
        f"[ALERT] 파이프라인 실패 알림\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Date: {exec_date}\n"
        f"Try: {try_number}/{max_tries}\n"
        f"Error: {str(error)[:200]}"
    )
    logger.error(alert_msg)

    slack = SlackAlert()
    slack.send_pipeline_failure(
        dag_id=dag_id,
        task_id=task_id,
        execution_date=exec_date,
        error_message=str(error)[:500] if error else "Unknown error",
    )


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA 위반 시 Slack 알림 (시뮬레이션)"""
    from scripts.slack_alert import SlackAlert

    missed_tasks = ", ".join([str(t) for t in task_list])
    alert_msg = (
        f"[SLA MISS] DAG: {dag.dag_id}\n"
        f"Tasks: {missed_tasks}\n"
        f"Blocking: {', '.join([str(t) for t in blocking_task_list])}"
    )
    logger.warning(alert_msg)

    slack = SlackAlert()
    slack.send_custom(
        title="[SLA MISS] 파이프라인 SLA 위반",
        message=alert_msg,
        color="#FFA500",
    )


def on_retry_callback(context):
    """Task 재시도 시 로그 기록"""
    ti = context.get("task_instance")
    logger.warning(
        f"[RETRY] Task '{ti.task_id}' 재시도 "
        f"({ti.try_number}/{ti.max_tries}) - "
        f"Error: {str(context.get('exception', 'N/A'))[:100]}"
    )


# ========== DAG 기본 설정 ==========

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
    "sla": timedelta(hours=2),
}

dag = DAG(
    dag_id="ecommerce_event_pipeline",
    default_args=default_args,
    description="이커머스 행동 로그 수집→검증→적재→마트 생성 파이프라인",
    schedule_interval="0 17 * * *",  # UTC 17:00 = KST 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "event", "pipeline", "dataops"],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    sla_miss_callback=on_sla_miss_callback,
    template_searchpath=[os.path.join(PROJECT_DIR, "sql")],
)


# ========== Task Functions ==========

def generate_events_task(**context):
    """이벤트 로그 생성"""
    from scripts.generate_events import EventGenerator

    execution_date = context["ds"]  # YYYY-MM-DD
    output_dir = os.path.join(PROJECT_DIR, "data", "events")

    generator = EventGenerator(target_date=execution_date, num_users=500)
    events = generator.generate()
    filepath = generator.save_to_json(output_dir)
    summary = generator.get_summary()

    # XCom으로 다음 Task에 전달
    context["ti"].xcom_push(key="events_filepath", value=filepath)
    context["ti"].xcom_push(key="events_count", value=summary["total_events"])
    context["ti"].xcom_push(key="events_summary", value=json.dumps(summary, ensure_ascii=False))

    print(f"[생성 완료] {execution_date}: {summary['total_events']:,}건 → {filepath}")
    return filepath


def validate_quality_task(**context):
    """데이터 품질 검증"""
    from scripts.validate_quality import validate_jsonl_file

    filepath = context["ti"].xcom_pull(task_ids="generate_events", key="events_filepath")
    report = validate_jsonl_file(filepath)

    context["ti"].xcom_push(key="quality_report", value=json.dumps(report, ensure_ascii=False))
    context["ti"].xcom_push(key="quality_status", value=report["overall_status"])

    print(f"[품질 검증] 상태: {report['overall_status']}")
    print(f"  통과: {report['passed']}/{report['total_checks']}")

    for check in report["checks"]:
        status_icon = "[PASS]" if check["status"] == "PASS" else "[FAIL]"
        print(f"  {status_icon} {check['check_name']}: {check['pass_rate']}%")

    return report["overall_status"]


def decide_on_quality(**context):
    """품질 검증 결과에 따라 분기 결정"""
    status = context["ti"].xcom_pull(task_ids="validate_quality", key="quality_status")

    if status == "PASS":
        return "load_to_database"
    else:
        return "quality_alert"


def load_to_database_task(**context):
    """검증 완료 이벤트를 DB에 적재 (Bulk Insert)"""
    filepath = context["ti"].xcom_pull(task_ids="generate_events", key="events_filepath")

    events = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))

    hook = PostgresHook(postgres_conn_id="ecommerce_db")

    insert_sql = """
        INSERT INTO raw_events (
            event_id, event_type, user_id, session_id, timestamp,
            platform, page_url, page_type, element_id, element_type,
            product_id, category_id, quantity, unit_price,
            order_id, total_amount, payment_method,
            search_query, result_count, referrer, device_type,
            os, browser, extra_data
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (event_id) DO NOTHING;
    """

    rows = []
    for e in events:
        rows.append((
            e.get("event_id"), e.get("event_type"), e.get("user_id"),
            e.get("session_id"), e.get("timestamp"), e.get("platform"),
            e.get("page_url"), e.get("page_type"), e.get("element_id"),
            e.get("element_type"), e.get("product_id"), e.get("category_id"),
            e.get("quantity"), e.get("unit_price"), e.get("order_id"),
            e.get("total_amount"), e.get("payment_method"),
            e.get("search_query"), e.get("result_count"),
            e.get("referrer"), e.get("device_type"),
            e.get("os"), e.get("browser"), e.get("extra_data"),
        ))

    # Batch insert (1000건 단위)
    batch_size = 1000
    conn = hook.get_conn()
    cursor = conn.cursor()
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[적재 완료] {len(rows):,}건 → raw_events")


def quality_alert_task(**context):
    """품질 검증 실패 시 Slack 알림"""
    from scripts.slack_alert import SlackAlert

    report_json = context["ti"].xcom_pull(task_ids="validate_quality", key="quality_report")
    report = json.loads(report_json)
    execution_date = context["ds"]

    failed_checks = [c for c in report["checks"] if c["status"] == "FAIL"]
    print(f"[WARN] 품질 검증 실패: {len(failed_checks)}건의 검증 항목이 실패했습니다.")
    for check in failed_checks:
        print(f"  [FAIL] {check['check_name']}: {check['pass_rate']}% (실패 {check['failed_records']}건)")
        print(f"     상세: {check['detail']}")

    slack = SlackAlert()
    slack.send_quality_report(
        execution_date=execution_date,
        total_checks=report["total_checks"],
        passed=report["passed"],
        failed_checks=[
            {"name": c["check_name"], "pass_rate": c["pass_rate"], "detail": c["detail"]}
            for c in failed_checks
        ],
    )


def save_quality_log_task(**context):
    """품질 검증 결과를 quality_check_log 테이블에 저장"""
    execution_date = context["ds"]
    report_json = context["ti"].xcom_pull(task_ids="validate_quality", key="quality_report")
    report = json.loads(report_json)

    hook = PostgresHook(postgres_conn_id="ecommerce_db")
    insert_sql = """
        INSERT INTO quality_check_log
            (check_date, check_name, check_type, target_table,
             total_records, failed_records, pass_rate, status, detail)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    for check in report["checks"]:
        cursor.execute(insert_sql, (
            execution_date,
            check["check_name"],
            check["check_type"],
            check["target_table"],
            check["total_records"],
            check["failed_records"],
            check["pass_rate"],
            check["status"],
            check.get("detail", ""),
        ))
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[품질 로그 저장] {len(report['checks'])}건 → quality_check_log")


def generate_quality_report_task(**context):
    """최종 품질 리포트 생성"""
    execution_date = context["ds"]
    events_summary = context["ti"].xcom_pull(task_ids="generate_events", key="events_summary")
    quality_report = context["ti"].xcom_pull(task_ids="validate_quality", key="quality_report")

    summary = json.loads(events_summary)
    quality = json.loads(quality_report)

    report = {
        "report_date": execution_date,
        "pipeline_status": "SUCCESS",
        "events": summary,
        "quality": quality,
        "mart_tables_refreshed": [
            "mart_user_daily",
            "mart_funnel_daily",
            "mart_product_daily",
            "mart_orders",
        ],
    }

    report_dir = os.path.join(PROJECT_DIR, "data", "reports")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"pipeline_report_{execution_date}.json")

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"파이프라인 리포트: {execution_date}")
    print(f"{'='*60}")
    print(f"  이벤트: {summary['total_events']:,}건")
    print(f"  사용자: {summary['unique_users']:,}명")
    print(f"  세션:   {summary['unique_sessions']:,}개")
    print(f"  구매:   {summary['purchase_count']:,}건")
    print(f"  매출:   ₩{summary['total_revenue']:,.0f}")
    print(f"  품질:   {quality['overall_status']} ({quality['passed']}/{quality['total_checks']})")
    print(f"{'='*60}")

    # Slack 완료 알림
    from scripts.slack_alert import SlackAlert
    slack = SlackAlert()
    slack.send_pipeline_success(
        execution_date=execution_date,
        total_events=summary["total_events"],
        unique_users=summary["unique_users"],
        quality_pass_rate=f"{quality['passed']}/{quality['total_checks']}",
    )


# ========== Task 정의 ==========

generate_events = PythonOperator(
    task_id="generate_events",
    python_callable=generate_events_task,
    execution_timeout=timedelta(minutes=30),
    sla=timedelta(minutes=20),
    dag=dag,
)

validate_quality = PythonOperator(
    task_id="validate_quality",
    python_callable=validate_quality_task,
    execution_timeout=timedelta(minutes=15),
    sla=timedelta(minutes=30),
    dag=dag,
)

quality_branch = BranchPythonOperator(
    task_id="quality_branch",
    python_callable=decide_on_quality,
    dag=dag,
)

load_to_database = PythonOperator(
    task_id="load_to_database",
    python_callable=load_to_database_task,
    retries=5,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(minutes=45),
    sla=timedelta(hours=1),
    dag=dag,
)

quality_alert = PythonOperator(
    task_id="quality_alert",
    python_callable=quality_alert_task,
    retries=1,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

# 마트 생성 Tasks (4종 병렬 실행, 개별 SQL 파일 참조)
create_user_mart = PostgresOperator(
    task_id="create_user_mart",
    postgres_conn_id="ecommerce_db",
    sql="marts/mart_user_daily.sql",
    retries=3,
    execution_timeout=timedelta(minutes=20),
    sla=timedelta(hours=1, minutes=30),
    dag=dag,
)

create_funnel_mart = PostgresOperator(
    task_id="create_funnel_mart",
    postgres_conn_id="ecommerce_db",
    sql="marts/mart_funnel_daily.sql",
    retries=3,
    execution_timeout=timedelta(minutes=20),
    sla=timedelta(hours=1, minutes=30),
    dag=dag,
)

create_product_mart = PostgresOperator(
    task_id="create_product_mart",
    postgres_conn_id="ecommerce_db",
    sql="marts/mart_product_daily.sql",
    retries=3,
    execution_timeout=timedelta(minutes=20),
    sla=timedelta(hours=1, minutes=30),
    dag=dag,
)

create_order_mart = PostgresOperator(
    task_id="create_order_mart",
    postgres_conn_id="ecommerce_db",
    sql="marts/mart_orders.sql",
    retries=3,
    execution_timeout=timedelta(minutes=20),
    sla=timedelta(hours=1, minutes=30),
    dag=dag,
)

save_quality_log = PythonOperator(
    task_id="save_quality_log",
    python_callable=save_quality_log_task,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

quality_report = PythonOperator(
    task_id="quality_report",
    python_callable=generate_quality_report_task,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)


# ========== Task 의존성 (DAG 그래프) ==========
#
#  generate_events → validate_quality → quality_branch
#                                          ├── [PASS] load_to_database
#                                          │     → [user_mart, funnel_mart, product_mart, order_mart]
#                                          │         → save_quality_log → quality_report
#                                          └── [FAIL] quality_alert
#                                                → save_quality_log → quality_report

generate_events >> validate_quality >> quality_branch

# PASS 경로: 적재 → 4종 마트 병렬 → 품질 로그 → 리포트
quality_branch >> load_to_database
load_to_database >> [create_user_mart, create_funnel_mart, create_product_mart, create_order_mart]
[create_user_mart, create_funnel_mart, create_product_mart, create_order_mart] >> save_quality_log

# FAIL 경로: 알림 → 품질 로그 → 리포트
quality_branch >> quality_alert >> save_quality_log

# 공통 종료: 품질 로그 → 리포트
save_quality_log >> quality_report
