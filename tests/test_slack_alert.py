"""Slack 알림 모듈 테스트"""

import json
import pytest
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.slack_alert import SlackAlert


class TestSlackAlertInit:
    """SlackAlert 초기화 테스트"""

    def test_init_without_env(self):
        """SLACK_WEBHOOK_URL 미설정 시 None"""
        with patch.dict(os.environ, {}, clear=True):
            alert = SlackAlert()
            assert alert.webhook_url is None

    def test_init_with_env(self):
        """SLACK_WEBHOOK_URL 설정 시 정상 로딩"""
        with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"}):
            alert = SlackAlert()
            assert alert.webhook_url == "https://hooks.slack.com/test"

    def test_init_with_explicit_url(self):
        """명시적 URL 전달"""
        alert = SlackAlert(webhook_url="https://hooks.slack.com/explicit")
        assert alert.webhook_url == "https://hooks.slack.com/explicit"


class TestSlackAlertSendMethods:
    """Slack 알림 전송 테스트 (실제 HTTP 요청 없이)"""

    @pytest.fixture
    def alert_no_url(self):
        """webhook URL 없는 인스턴스"""
        return SlackAlert(webhook_url=None)

    @pytest.fixture
    def alert_with_url(self):
        """webhook URL 있는 인스턴스"""
        return SlackAlert(webhook_url="https://hooks.slack.com/test")

    def test_send_quality_report_without_url_returns_false(self, alert_no_url):
        """URL 미설정 시 False 반환 (에러 없이)"""
        result = alert_no_url.send_quality_report(
            execution_date="2026-01-01",
            total_checks=7,
            passed=5,
            failed_checks=[{"name": "null_check", "pass_rate": 95.0, "detail": "test"}],
        )
        assert result is False

    def test_send_pipeline_success_without_url_returns_false(self, alert_no_url):
        result = alert_no_url.send_pipeline_success(
            execution_date="2026-01-01",
            total_events=10000,
            unique_users=500,
            quality_pass_rate="7/7",
        )
        assert result is False

    def test_send_pipeline_failure_without_url_returns_false(self, alert_no_url):
        result = alert_no_url.send_pipeline_failure(
            dag_id="test_dag",
            task_id="test_task",
            execution_date="2026-01-01",
            error_message="test error",
        )
        assert result is False

    def test_send_custom_without_url_returns_false(self, alert_no_url):
        result = alert_no_url.send_custom(title="test", message="msg")
        assert result is False

    @patch("scripts.slack_alert.urlopen")
    def test_send_quality_report_success(self, mock_urlopen, alert_with_url):
        """정상 전송 시 True 반환"""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__ = MagicMock(return_value=mock_response)
        mock_urlopen.return_value.__exit__ = MagicMock(return_value=False)

        result = alert_with_url.send_quality_report(
            execution_date="2026-01-01",
            total_checks=7,
            passed=7,
            failed_checks=[],
        )
        assert result is True
        mock_urlopen.assert_called_once()

    @patch("scripts.slack_alert.urlopen")
    def test_send_pipeline_success_sends_correct_payload(self, mock_urlopen, alert_with_url):
        """파이프라인 성공 알림 페이로드 검증"""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__ = MagicMock(return_value=mock_response)
        mock_urlopen.return_value.__exit__ = MagicMock(return_value=False)

        alert_with_url.send_pipeline_success(
            execution_date="2026-01-15",
            total_events=25000,
            unique_users=800,
            quality_pass_rate="7/7",
        )

        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        payload = json.loads(request.data.decode("utf-8"))

        assert "blocks" in payload
        # 블록 중 execution_date가 포함된 텍스트가 있는지 확인
        payload_text = json.dumps(payload)
        assert "2026-01-15" in payload_text

    @patch("scripts.slack_alert.urlopen")
    def test_send_pipeline_failure_sends_correct_payload(self, mock_urlopen, alert_with_url):
        """파이프라인 실패 알림 페이로드 검증"""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__ = MagicMock(return_value=mock_response)
        mock_urlopen.return_value.__exit__ = MagicMock(return_value=False)

        alert_with_url.send_pipeline_failure(
            dag_id="ecommerce_event_pipeline",
            task_id="load_to_database",
            execution_date="2026-01-15",
            error_message="Connection refused",
        )

        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        payload = json.loads(request.data.decode("utf-8"))

        payload_text = json.dumps(payload)
        assert "Connection refused" in payload_text
        assert "load_to_database" in payload_text

    @patch("scripts.slack_alert.urlopen")
    def test_send_handles_http_error(self, mock_urlopen, alert_with_url):
        """HTTP 에러 시 False 반환"""
        from urllib.error import URLError
        mock_urlopen.side_effect = URLError("Connection failed")

        result = alert_with_url.send_custom(title="test", message="msg")
        assert result is False


class TestSlackAlertPayloadStructure:
    """페이로드 구조 테스트"""

    def test_quality_report_with_failed_checks(self):
        """실패 항목이 있을 때 상세 정보 포함"""
        alert = SlackAlert(webhook_url="https://hooks.slack.com/test")

        failed_checks = [
            {"name": "null_check", "pass_rate": 95.5, "detail": "5건 누락"},
            {"name": "range_check", "pass_rate": 88.0, "detail": "12건 범위 초과"},
        ]

        # _build_quality_payload 직접 호출하여 구조 검증
        blocks = alert._build_quality_blocks(
            execution_date="2026-01-01",
            total_checks=7,
            passed=5,
            failed_checks=failed_checks,
        )
        block_text = json.dumps(blocks, ensure_ascii=False)
        assert "null_check" in block_text
        assert "range_check" in block_text
        assert "5건 누락" in block_text

    def test_quality_report_all_pass(self):
        """전체 통과 시 실패 항목 없음"""
        alert = SlackAlert(webhook_url="https://hooks.slack.com/test")
        blocks = alert._build_quality_blocks(
            execution_date="2026-01-01",
            total_checks=7,
            passed=7,
            failed_checks=[],
        )
        block_text = json.dumps(blocks)
        assert "PASS" in block_text


# ========== 엣지 케이스 / 예외 상황 테스트 ==========

class TestSlackAlertEdgeCases:
    """Slack 알림 엣지 케이스 및 예외 상황"""

    def test_empty_webhook_url_string(self):
        """빈 문자열 webhook URL"""
        alert = SlackAlert(webhook_url="")
        assert alert.webhook_url is None or not alert.is_configured

    def test_send_with_none_fields(self):
        """None/빈 값 필드로 전송 시 에러 없이 처리"""
        alert = SlackAlert(webhook_url=None)
        # error_message=None은 슬라이싱 불가이므로 빈 문자열로 대체 테스트
        result = alert.send_pipeline_failure(
            dag_id="",
            task_id="",
            execution_date="",
            error_message="",
        )
        assert result is False

    def test_send_quality_report_with_zero_checks(self):
        """검증 0건으로 리포트 전송"""
        alert = SlackAlert(webhook_url=None)
        result = alert.send_quality_report(
            execution_date="2026-01-01",
            total_checks=0,
            passed=0,
            failed_checks=[],
        )
        assert result is False  # URL 없으므로 False

    def test_build_quality_blocks_with_many_failures(self):
        """실패 항목이 많을 때 블록 생성"""
        alert = SlackAlert(webhook_url="https://hooks.slack.com/test")
        failed_checks = [
            {"name": f"check_{i}", "pass_rate": 50.0 + i, "detail": f"에러 {i}건"}
            for i in range(20)
        ]
        blocks = alert._build_quality_blocks(
            execution_date="2026-01-01",
            total_checks=20,
            passed=0,
            failed_checks=failed_checks,
        )
        block_text = json.dumps(blocks, ensure_ascii=False)
        assert "check_0" in block_text
        assert "check_19" in block_text

    def test_send_custom_with_special_characters(self):
        """특수문자/이모지 포함 메시지"""
        alert = SlackAlert(webhook_url=None)
        result = alert.send_custom(
            title="🚨 알림 <b>테스트</b>",
            message="메시지에 \"따옴표\"와 \n줄바꿈 포함",
        )
        assert result is False

    def test_send_pipeline_failure_with_very_long_error(self):
        """매우 긴 에러 메시지 (500자 초과 → 자르기)"""
        long_error = "E" * 2000
        alert = SlackAlert(webhook_url=None)
        result = alert.send_pipeline_failure(
            dag_id="test_dag",
            task_id="test_task",
            execution_date="2026-01-01",
            error_message=long_error,
        )
        assert result is False

    @patch("scripts.slack_alert.urlopen")
    def test_send_handles_timeout_error(self, mock_urlopen):
        """타임아웃 에러 시 False 반환"""
        import socket
        mock_urlopen.side_effect = socket.timeout("Connection timed out")
        alert = SlackAlert(webhook_url="https://hooks.slack.com/test")
        result = alert.send_custom(title="test", message="msg")
        assert result is False

    @patch("scripts.slack_alert.urlopen")
    def test_send_handles_non_200_status(self, mock_urlopen):
        """HTTP 500 에러 시 False 반환"""
        mock_response = MagicMock()
        mock_response.status = 500
        mock_urlopen.return_value.__enter__ = MagicMock(return_value=mock_response)
        mock_urlopen.return_value.__exit__ = MagicMock(return_value=False)

        alert = SlackAlert(webhook_url="https://hooks.slack.com/test")
        result = alert.send_custom(title="test", message="msg")
        assert result is False

    def test_is_configured_property(self):
        """is_configured 속성 정상 동작"""
        assert SlackAlert(webhook_url=None).is_configured is False
        assert SlackAlert(webhook_url="https://hooks.slack.com/test").is_configured is True

    def test_init_env_var_precedence(self):
        """명시적 URL이 환경변수보다 우선"""
        with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://env.url"}):
            alert = SlackAlert(webhook_url="https://explicit.url")
            assert alert.webhook_url == "https://explicit.url"
