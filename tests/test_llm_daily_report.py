"""
LLM 일간 매출 리포트 생성 모듈 테스트
====================================
LLMClient, MartDataExtractor, LLMDailyReporter 의 단위 테스트.
외부 API 의존 없이 Mock 기반으로 검증합니다.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from scripts.llm_daily_report import (
    LLMClient,
    LLMDailyReporter,
    MartDataExtractor,
    SYSTEM_PROMPT,
)


# ========== Fixtures ==========

@pytest.fixture
def sample_mart_data():
    """테스트용 마트 데이터"""
    return {
        "user_mart": {
            "active_users": 500,
            "total_sessions": 1200,
            "total_page_views": 8500,
            "total_clicks": 3200,
            "total_add_to_carts": 650,
            "total_purchases": 120,
            "total_revenue": 15_800_000,
            "avg_purchase_amount": 131_666,
            "purchasing_users": 95,
        },
        "funnel_mart": [
            {
                "platform": "web",
                "viewers": 300,
                "clickers": 180,
                "cart_adders": 85,
                "purchasers": 45,
                "view_to_click_rate": 60.0,
                "click_to_cart_rate": 47.2,
                "cart_to_purchase_rate": 52.9,
                "overall_conversion_rate": 15.0,
            },
            {
                "platform": "ios",
                "viewers": 120,
                "clickers": 80,
                "cart_adders": 40,
                "purchasers": 20,
                "view_to_click_rate": 66.7,
                "click_to_cart_rate": 50.0,
                "cart_to_purchase_rate": 50.0,
                "overall_conversion_rate": 16.7,
            },
        ],
        "product_mart": [
            {
                "product_id": "P001",
                "click_count": 150,
                "cart_add_count": 45,
                "purchase_count": 20,
                "revenue": 2_500_000,
                "units_sold": 25,
                "unique_viewers": 120,
                "unique_buyers": 18,
                "conversion_rate": 15.0,
            },
            {
                "product_id": "P002",
                "click_count": 100,
                "cart_add_count": 30,
                "purchase_count": 12,
                "revenue": 1_800_000,
                "units_sold": 15,
                "unique_viewers": 85,
                "unique_buyers": 11,
                "conversion_rate": 12.9,
            },
        ],
        "order_mart_summary": {
            "order_count": 120,
            "total_revenue": 15_800_000,
            "avg_order_value": 131_666,
            "max_order_amount": 850_000,
            "unique_buyers": 95,
            "payment_method_count": 5,
        },
        "payment_distribution": [
            {"method": "credit_card", "order_count": 50, "total_amount": 7_200_000},
            {"method": "kakao_pay", "order_count": 30, "total_amount": 3_800_000},
            {"method": "naver_pay", "order_count": 20, "total_amount": 2_500_000},
        ],
    }


@pytest.fixture
def sample_report_json(tmp_path):
    """테스트용 pipeline_report JSON 파일"""
    report = {
        "report_date": "2026-01-15",
        "pipeline_status": "SUCCESS",
        "events": {
            "total_events": 5000,
            "unique_users": 500,
            "unique_sessions": 1200,
            "purchase_count": 120,
            "total_revenue": 15_800_000,
            "event_breakdown": {
                "page_view": 2500,
                "click": 1200,
                "add_to_cart": 600,
                "purchase": 120,
                "search": 580,
            },
        },
    }
    filepath = tmp_path / "pipeline_report_2026-01-15.json"
    filepath.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    return str(filepath)


# ========== LLMClient Tests ==========

class TestLLMClient:
    """LLMClient 테스트"""

    def test_init_openai_default(self):
        """OpenAI 프로바이더 기본 초기화"""
        client = LLMClient(provider="openai", api_key="test-key")
        assert client.provider == "openai"
        assert client.model == "gpt-4o-mini"
        assert client.is_configured is True

    def test_init_anthropic_default(self):
        """Anthropic 프로바이더 기본 초기화"""
        client = LLMClient(provider="anthropic", api_key="test-key")
        assert client.provider == "anthropic"
        assert client.model == "claude-3-5-sonnet-20241022"
        assert client.is_configured is True

    def test_init_custom_model(self):
        """커스텀 모델 지정"""
        client = LLMClient(provider="openai", api_key="key", model="gpt-4o")
        assert client.model == "gpt-4o"

    def test_init_invalid_provider(self):
        """잘못된 프로바이더 → ValueError"""
        with pytest.raises(ValueError, match="지원하지 않는 LLM provider"):
            LLMClient(provider="invalid")

    def test_not_configured_without_key(self):
        """API 키 없으면 is_configured = False"""
        with patch.dict(os.environ, {}, clear=True):
            client = LLMClient(provider="openai")
            # 환경변수도 없으면 False
            if not os.environ.get("OPENAI_API_KEY"):
                assert client.is_configured is False

    def test_fallback_when_no_key(self):
        """API 키 미설정 시 폴백 리포트 반환"""
        client = LLMClient(provider="openai", api_key=None)
        # 강제로 api_key를 None으로
        client.api_key = None
        result = client.generate("system", "user prompt data")
        assert "폴백 모드" in result
        assert "user prompt data" in result

    def test_generate_fallback_contains_data(self):
        """폴백 리포트에 원본 데이터 포함"""
        client = LLMClient(provider="openai", api_key="key")
        fallback = client._generate_fallback("매출 데이터 요약")
        assert "매출 데이터 요약" in fallback
        assert "API 키" in fallback


# ========== MartDataExtractor Tests ==========

class TestMartDataExtractor:
    """MartDataExtractor 테스트"""

    def test_extract_from_report(self, sample_report_json):
        """로컬 리포트 파일에서 데이터 추출"""
        data = MartDataExtractor.extract_from_report(sample_report_json)

        assert "user_mart" in data
        assert "funnel_mart" in data
        assert "product_mart" in data
        assert "order_mart_summary" in data

        user = data["user_mart"]
        assert user["active_users"] == 500
        assert user["total_sessions"] == 1200
        assert user["total_revenue"] == 15_800_000
        assert user["total_purchases"] == 120

    def test_extract_from_report_order_summary(self, sample_report_json):
        """로컬 리포트에서 주문 마트 요약 추출"""
        data = MartDataExtractor.extract_from_report(sample_report_json)
        order = data["order_mart_summary"]
        assert order["order_count"] == 120
        assert order["total_revenue"] == 15_800_000

    def test_extract_from_db_with_mock(self):
        """DB 추출 (Mock Hook)"""
        mock_hook = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # 순서대로 fetchone/fetchall 결과 설정
        mock_cursor.fetchone.side_effect = [
            (500, 1200, 8500, 3200, 650, 120, 15800000, 131666, 95),  # user_mart
            (120, 15800000, 131666, 850000, 95, 5),  # order_mart
        ]
        mock_cursor.fetchall.side_effect = [
            [("web", 300, 180, 85, 45, 60.0, 47.2, 52.9, 15.0)],  # funnel
            [("P001", 150, 45, 20, 2500000, 25, 120, 18, 15.0)],  # product
            [("credit_card", 50, 7200000)],  # payment
        ]

        data = MartDataExtractor.extract_from_db(mock_hook, "2026-01-15")
        assert data["user_mart"]["active_users"] == 500
        assert data["user_mart"]["total_revenue"] == 15800000
        assert len(data["funnel_mart"]) == 1
        assert data["funnel_mart"][0]["platform"] == "web"
        assert len(data["product_mart"]) == 1
        assert data["order_mart_summary"]["order_count"] == 120


# ========== LLMDailyReporter Tests ==========

class TestLLMDailyReporter:
    """LLMDailyReporter 테스트"""

    def test_init_default(self):
        """기본 초기화 (OpenAI)"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        assert reporter.provider == "openai"
        assert reporter.llm.model == "gpt-4o-mini"

    def test_build_prompt(self, sample_mart_data):
        """프롬프트 빌드 — 모든 섹션 포함 확인"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        prompt = reporter.build_prompt(sample_mart_data, "2026-01-15")

        assert "2026-01-15" in prompt
        assert "사용자 마트" in prompt
        assert "전환 퍼널 마트" in prompt
        assert "상품 마트" in prompt
        assert "주문 마트" in prompt
        assert "500" in prompt  # active_users
        assert "15,800,000" in prompt  # total_revenue
        assert "P001" in prompt  # product_id
        assert "credit_card" in prompt  # payment method

    def test_build_prompt_empty_data(self):
        """빈 데이터로 프롬프트 생성"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        prompt = reporter.build_prompt({}, "2026-01-15")
        assert "2026-01-15" in prompt
        assert "데이터 없음" in prompt

    def test_format_user_mart(self, sample_mart_data):
        """사용자 마트 포맷팅"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        text = reporter._format_user_mart(sample_mart_data["user_mart"])
        assert "500" in text
        assert "15,800,000" in text
        assert "활성 사용자" in text

    def test_format_funnel_mart(self, sample_mart_data):
        """퍼널 마트 포맷팅"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        text = reporter._format_funnel_mart(sample_mart_data["funnel_mart"])
        assert "WEB" in text
        assert "IOS" in text
        assert "60.0%" in text

    def test_format_product_mart(self, sample_mart_data):
        """상품 마트 포맷팅"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        text = reporter._format_product_mart(sample_mart_data["product_mart"])
        assert "P001" in text
        assert "2,500,000" in text

    def test_format_order_mart(self, sample_mart_data):
        """주문 마트 포맷팅"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        text = reporter._format_order_mart(
            sample_mart_data["order_mart_summary"],
            sample_mart_data["payment_distribution"],
        )
        assert "120" in text
        assert "credit_card" in text

    def test_generate_report_fallback(self, sample_mart_data):
        """API 키 없을 때 폴백 리포트 생성"""
        reporter = LLMDailyReporter(provider="openai", api_key=None)
        reporter.llm.api_key = None
        report = reporter.generate_report(sample_mart_data, "2026-01-15")
        assert len(report) > 0
        assert "폴백" in report or "2026-01-15" in report

    def test_save_report(self, sample_mart_data):
        """리포트 파일 저장"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = reporter.save_report("테스트 리포트 내용", "2026-01-15", tmpdir)
            assert os.path.exists(path)
            assert "llm_report_2026-01-15.json" in path

            with open(path, "r", encoding="utf-8") as f:
                saved = json.load(f)
            assert saved["report_text"] == "테스트 리포트 내용"
            assert saved["execution_date"] == "2026-01-15"
            assert saved["provider"] == "openai"

    def test_send_to_slack_without_webhook(self, sample_mart_data):
        """Slack webhook 미설정 시 로그 폴백"""
        reporter = LLMDailyReporter(provider="openai", api_key="test")
        # webhook URL 없으면 False 반환 (로그 폴백)
        result = reporter.send_to_slack("리포트 내용", "2026-01-15")
        assert result is False  # webhook 미설정이므로


# ========== 통합 테스트 ==========

class TestIntegration:
    """통합 테스트"""

    def test_full_pipeline_local(self, sample_report_json):
        """로컬 리포트 → 프롬프트 → 폴백 리포트 전체 흐름"""
        # 1. 로컬 리포트에서 데이터 추출
        mart_data = MartDataExtractor.extract_from_report(sample_report_json)

        # 2. 리포터 생성 (API 키 없이 폴백 모드)
        reporter = LLMDailyReporter(provider="openai", api_key=None)
        reporter.llm.api_key = None

        # 3. 프롬프트 빌드 확인
        prompt = reporter.build_prompt(mart_data, "2026-01-15")
        assert "2026-01-15" in prompt
        assert "500" in prompt

        # 4. 리포트 생성 (폴백)
        report = reporter.generate_report(mart_data, "2026-01-15")
        assert len(report) > 100

        # 5. 저장
        with tempfile.TemporaryDirectory() as tmpdir:
            path = reporter.save_report(report, "2026-01-15", tmpdir)
            assert os.path.exists(path)

    def test_system_prompt_completeness(self):
        """시스템 프롬프트에 필요 지시사항 포함 확인"""
        assert "이커머스" in SYSTEM_PROMPT
        assert "매출" in SYSTEM_PROMPT
        assert "전환율" in SYSTEM_PROMPT
        assert "한국어" in SYSTEM_PROMPT
        assert "액션 아이템" in SYSTEM_PROMPT
        assert "₩" in SYSTEM_PROMPT
