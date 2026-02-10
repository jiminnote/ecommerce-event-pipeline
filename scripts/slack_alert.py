"""
Slack 알림 모듈
====================================
파이프라인 이벤트(성공, 실패, 품질 검증 결과)를
Slack Incoming Webhook으로 전송합니다.

설정:
  환경변수 SLACK_WEBHOOK_URL 또는 생성자에 webhook_url 직접 전달

사용법:
  from scripts.slack_alert import SlackAlert

  alert = SlackAlert()
  alert.send_quality_report(execution_date="2026-01-01", total_checks=7, passed=5, ...)
  alert.send_pipeline_failure(dag_id="...", task_id="...", ...)
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)


class SlackAlert:
    """Slack Incoming Webhook 기반 알림 발송기"""

    def __init__(self, webhook_url: str = None, channel: str = "#data-alerts"):
        """
        Args:
            webhook_url: Slack Incoming Webhook URL.
                         미지정 시 환경변수 SLACK_WEBHOOK_URL 사용.
                         둘 다 없으면 None (로그 fallback).
            channel: 발송 대상 채널
        """
        self.webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL") or None
        self.channel = channel

    @property
    def is_configured(self) -> bool:
        return bool(self.webhook_url)

    def _post(self, payload: Dict) -> bool:
        """Slack webhook에 payload를 POST"""
        if not self.is_configured:
            logger.warning(
                "[SlackAlert] webhook URL 미설정 -- "
                "SLACK_WEBHOOK_URL 환경변수 또는 생성자 인자로 지정하세요. "
                "메시지를 로그로 대체합니다."
            )
            logger.info("[SlackAlert] payload: %s", json.dumps(payload, ensure_ascii=False))
            return False

        data = json.dumps(payload).encode("utf-8")
        req = Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
        )

        try:
            with urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    logger.info("[SlackAlert] 발송 성공")
                    return True
                else:
                    logger.error("[SlackAlert] 발송 실패: HTTP %d", resp.status)
                    return False
        except (URLError, Exception) as e:
            logger.error("[SlackAlert] 발송 실패: %s", str(e))
            return False

    def _build_quality_blocks(
        self,
        execution_date: str,
        total_checks: int,
        passed: int,
        failed_checks: List[Dict],
    ) -> List[Dict]:
        """품질 검증 알림용 Block Kit 블록 생성"""
        is_pass = passed == total_checks
        status_text = "PASS" if is_pass else "FAIL"
        header = f"[{status_text}] 품질 검증 결과 ({execution_date})"

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": header}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*검증 결과:* {passed}/{total_checks} 통과"},
                    {"type": "mrkdwn", "text": f"*날짜:* {execution_date}"},
                ],
            },
        ]

        if failed_checks:
            detail_lines = []
            for fc in failed_checks:
                detail_lines.append(
                    f"- *{fc['name']}*: {fc['pass_rate']}% ({fc.get('detail', '')})"
                )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*실패 항목:*\n" + "\n".join(detail_lines)},
            })

        return blocks

    def send_quality_report(
        self,
        execution_date: str,
        total_checks: int,
        passed: int,
        failed_checks: List[Dict] = None,
    ) -> bool:
        """품질 검증 결과 알림

        Args:
            execution_date: 실행 일자 (YYYY-MM-DD)
            total_checks: 전체 검증 항목 수
            passed: 통과 항목 수
            failed_checks: 실패 항목 리스트 [{"name": ..., "pass_rate": ..., "detail": ...}]
        """
        failed_checks = failed_checks or []
        blocks = self._build_quality_blocks(execution_date, total_checks, passed, failed_checks)

        payload = {"channel": self.channel, "blocks": blocks}
        return self._post(payload)

    def send_pipeline_success(
        self,
        execution_date: str,
        total_events: int,
        unique_users: int,
        quality_pass_rate: str,
    ) -> bool:
        """파이프라인 정상 완료 알림"""
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"[SUCCESS] 파이프라인 완료 ({execution_date})"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*이벤트:* {total_events:,}건"},
                    {"type": "mrkdwn", "text": f"*사용자:* {unique_users:,}명"},
                    {"type": "mrkdwn", "text": f"*품질:* {quality_pass_rate}"},
                    {"type": "mrkdwn", "text": f"*날짜:* {execution_date}"},
                ],
            },
        ]

        payload = {"channel": self.channel, "blocks": blocks}
        return self._post(payload)

    def send_pipeline_failure(
        self,
        dag_id: str,
        task_id: str,
        execution_date: str,
        error_message: str,
    ) -> bool:
        """파이프라인 Task 실패 알림"""
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "[ALERT] 파이프라인 실패"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:* {dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task:* {task_id}"},
                    {"type": "mrkdwn", "text": f"*Date:* {execution_date}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error:*\n```{error_message[:500]}```"},
            },
        ]

        payload = {"channel": self.channel, "blocks": blocks}
        return self._post(payload)

    def send_custom(self, title: str, message: str, color: str = "#439FE0") -> bool:
        """범용 커스텀 알림"""
        payload = {
            "channel": self.channel,
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "footer": "ecommerce-event-pipeline",
                    "ts": int(datetime.now().timestamp()),
                }
            ],
        }
        return self._post(payload)
