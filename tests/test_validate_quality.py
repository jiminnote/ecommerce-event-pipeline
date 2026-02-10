"""
데이터 품질 검증기 테스트
====================================
- 각 검증 항목별 정상/비정상 케이스
- 전체 검증 파이프라인 통합 테스트
- 리포트 구조 검증
"""

import pytest
import json
import uuid
import os
import tempfile
from copy import deepcopy

from scripts.validate_quality import EventQualityValidator, validate_jsonl_file


# ========== 테스트 헬퍼 ==========

def make_event(event_type="page_view", **overrides):
    """테스트용 이벤트 생성 헬퍼"""
    base = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": "U000001",
        "session_id": str(uuid.uuid4()),
        "timestamp": "2026-01-15T10:30:00",
        "platform": "web",
        "device_type": "desktop",
    }
    type_fields = {
        "page_view": {
            "page_url": "https://shop.example.com/home",
            "page_type": "home",
        },
        "click": {
            "page_url": "https://shop.example.com/product/P1001",
            "element_id": "btn_add",
            "element_type": "button",
            "product_id": "P1001",
            "category_id": "CAT001",
        },
        "add_to_cart": {
            "product_id": "P1001",
            "category_id": "CAT001",
            "quantity": 1,
            "unit_price": 89000,
        },
        "purchase": {
            "order_id": "ORD2026011500001",
            "total_amount": 89000,
            "payment_method": "toss_pay",
            "extra_data": json.dumps({
                "products": [{"product_id": "P1001", "quantity": 1, "unit_price": 89000}],
                "discount_amount": 0,
                "shipping_fee": 0,
            }),
        },
        "search": {
            "search_query": "이어폰",
            "result_count": 15,
            "page_url": "https://shop.example.com/search",
        },
    }
    base.update(type_fields.get(event_type, {}))
    base.update(overrides)
    return base


def make_session_events(session_id=None):
    """정상적인 전환 퍼널 세션 이벤트 세트 생성"""
    sid = session_id or str(uuid.uuid4())
    return [
        make_event("page_view",   session_id=sid, timestamp="2026-01-15T10:00:00"),
        make_event("click",       session_id=sid, timestamp="2026-01-15T10:01:00"),
        make_event("add_to_cart", session_id=sid, timestamp="2026-01-15T10:02:00"),
        make_event("purchase",    session_id=sid, timestamp="2026-01-15T10:03:00"),
        make_event("search",      timestamp="2026-01-15T10:04:00"),
    ]


# ========== NULL 검사 테스트 ==========

class TestNullCheck:
    def test_pass_with_valid_events(self):
        events = [make_event("page_view"), make_event("click"), make_event("search")]
        validator = EventQualityValidator(events)
        result = validator.check_null_fields()
        assert result.status == "PASS"
        assert result.failed_records == 0

    def test_fail_missing_event_id(self):
        events = [make_event("page_view", event_id=None)]
        validator = EventQualityValidator(events)
        result = validator.check_null_fields()
        assert result.failed_records > 0

    def test_fail_missing_user_id(self):
        events = [make_event("page_view", user_id="")]
        validator = EventQualityValidator(events)
        result = validator.check_null_fields()
        assert result.failed_records > 0

    def test_fail_missing_type_specific_field(self):
        """이벤트 유형별 필수 필드 누락 탐지"""
        events = [make_event("add_to_cart", product_id=None)]
        validator = EventQualityValidator(events)
        result = validator.check_null_fields()
        assert result.failed_records > 0


# ========== 중복 검사 테스트 ==========

class TestDuplicateCheck:
    def test_pass_with_unique_ids(self):
        events = [make_event("page_view") for _ in range(10)]
        validator = EventQualityValidator(events)
        result = validator.check_duplicates()
        assert result.status == "PASS"
        assert result.failed_records == 0

    def test_fail_with_duplicate_ids(self):
        dup_id = str(uuid.uuid4())
        events = [
            make_event("page_view", event_id=dup_id),
            make_event("click", event_id=dup_id),
            make_event("search", event_id=dup_id),
        ]
        validator = EventQualityValidator(events)
        result = validator.check_duplicates()
        assert result.failed_records == 2  # 3개 중 2개가 중복


# ========== 범위 검사 테스트 ==========

class TestRangeCheck:
    def test_pass_with_valid_values(self):
        events = [
            make_event("add_to_cart", quantity=2, unit_price=50000),
            make_event("purchase", total_amount=100000),
            make_event("search", result_count=42),
        ]
        validator = EventQualityValidator(events)
        result = validator.check_value_ranges()
        assert result.status == "PASS"

    def test_fail_zero_quantity(self):
        events = [make_event("add_to_cart", quantity=0)]
        validator = EventQualityValidator(events)
        result = validator.check_value_ranges()
        assert result.failed_records > 0

    def test_fail_negative_price(self):
        events = [make_event("add_to_cart", unit_price=-1000)]
        validator = EventQualityValidator(events)
        result = validator.check_value_ranges()
        assert result.failed_records > 0

    def test_fail_excessive_amount(self):
        events = [make_event("purchase", total_amount=999_999_999)]
        validator = EventQualityValidator(events)
        result = validator.check_value_ranges()
        assert result.failed_records > 0

    def test_fail_negative_search_results(self):
        events = [make_event("search", result_count=-1)]
        validator = EventQualityValidator(events)
        result = validator.check_value_ranges()
        assert result.failed_records > 0

    def test_fail_invalid_event_type(self):
        events = [make_event(event_type="unknown_event")]
        validator = EventQualityValidator(events)
        result = validator.check_value_ranges()
        assert result.failed_records > 0


# ========== 퍼널 순서 검사 테스트 ==========

class TestSequenceCheck:
    def test_pass_correct_funnel(self):
        events = make_session_events()
        validator = EventQualityValidator(events)
        result = validator.check_event_sequence()
        assert result.status == "PASS"

    def test_fail_purchase_without_cart(self):
        sid = str(uuid.uuid4())
        events = [
            make_event("page_view", session_id=sid, timestamp="2026-01-15T10:00:00"),
            make_event("click",     session_id=sid, timestamp="2026-01-15T10:01:00"),
            make_event("purchase",  session_id=sid, timestamp="2026-01-15T10:02:00"),
        ]
        validator = EventQualityValidator(events)
        result = validator.check_event_sequence()
        assert result.failed_records > 0

    def test_pass_session_without_purchase(self):
        """구매가 없는 세션은 정상"""
        sid = str(uuid.uuid4())
        events = [
            make_event("page_view", session_id=sid, timestamp="2026-01-15T10:00:00"),
            make_event("click",     session_id=sid, timestamp="2026-01-15T10:01:00"),
        ]
        validator = EventQualityValidator(events)
        result = validator.check_event_sequence()
        assert result.status == "PASS"


# ========== 타임스탬프 검사 테스트 ==========

class TestTimestampCheck:
    def test_pass_valid_timestamp(self):
        events = [make_event("page_view", timestamp="2026-01-15T10:30:00")]
        validator = EventQualityValidator(events)
        result = validator.check_timestamp_validity()
        assert result.status == "PASS"

    def test_fail_invalid_format(self):
        events = [make_event("page_view", timestamp="not-a-date")]
        validator = EventQualityValidator(events)
        result = validator.check_timestamp_validity()
        assert result.failed_records == 1

    def test_fail_null_timestamp(self):
        events = [make_event("page_view", timestamp=None)]
        validator = EventQualityValidator(events)
        result = validator.check_timestamp_validity()
        assert result.failed_records == 1

    def test_fail_out_of_range_year(self):
        events = [make_event("page_view", timestamp="2015-01-15T10:30:00")]
        validator = EventQualityValidator(events)
        result = validator.check_timestamp_validity()
        assert result.failed_records == 1


# ========== 금액 정합성 검사 테스트 ==========

class TestCrossReferenceCheck:
    def test_pass_matching_amounts(self):
        events = [make_event("purchase",
            total_amount=89000,
            extra_data=json.dumps({
                "products": [{"product_id": "P1001", "quantity": 1, "unit_price": 89000}],
                "discount_amount": 0,
                "shipping_fee": 0,
            })
        )]
        validator = EventQualityValidator(events)
        result = validator.check_cross_reference()
        assert result.status == "PASS"

    def test_pass_with_discount_and_shipping(self):
        """할인 + 배송비 포함 정합성"""
        events = [make_event("purchase",
            total_amount=87000,  # 89000 - 5000 + 3000
            extra_data=json.dumps({
                "products": [{"product_id": "P1001", "quantity": 1, "unit_price": 89000}],
                "discount_amount": 5000,
                "shipping_fee": 3000,
            })
        )]
        validator = EventQualityValidator(events)
        result = validator.check_cross_reference()
        assert result.status == "PASS"

    def test_fail_mismatched_amounts(self):
        events = [make_event("purchase",
            total_amount=999999,
            extra_data=json.dumps({
                "products": [{"product_id": "P1001", "quantity": 1, "unit_price": 89000}],
                "discount_amount": 0,
                "shipping_fee": 0,
            })
        )]
        validator = EventQualityValidator(events)
        result = validator.check_cross_reference()
        assert result.failed_records == 1

    def test_fail_invalid_extra_data_json(self):
        events = [make_event("purchase", extra_data="invalid json {{{")]
        validator = EventQualityValidator(events)
        result = validator.check_cross_reference()
        assert result.failed_records == 1


# ========== 플랫폼-디바이스 정합성 검사 테스트 ==========

class TestPlatformDeviceCheck:
    def test_pass_valid_combinations(self):
        events = [
            make_event("page_view", platform="ios", device_type="mobile"),
            make_event("page_view", platform="android", device_type="mobile"),
            make_event("page_view", platform="web", device_type="desktop"),
            make_event("page_view", platform="web", device_type="tablet"),
        ]
        validator = EventQualityValidator(events)
        result = validator.check_platform_device_consistency()
        assert result.status == "PASS"

    def test_fail_ios_desktop(self):
        events = [make_event("page_view", platform="ios", device_type="desktop")]
        validator = EventQualityValidator(events)
        result = validator.check_platform_device_consistency()
        assert result.failed_records > 0

    def test_fail_android_tablet(self):
        events = [make_event("page_view", platform="android", device_type="tablet")]
        validator = EventQualityValidator(events)
        result = validator.check_platform_device_consistency()
        assert result.failed_records > 0

    def test_fail_session_platform_change(self):
        """동일 세션 내 플랫폼 변경 탐지"""
        sid = str(uuid.uuid4())
        events = [
            make_event("page_view", session_id=sid, platform="ios", device_type="mobile",
                       timestamp="2026-01-15T10:00:00"),
            make_event("click", session_id=sid, platform="web", device_type="desktop",
                       timestamp="2026-01-15T10:01:00"),
        ]
        validator = EventQualityValidator(events)
        result = validator.check_platform_device_consistency()
        assert result.failed_records > 0


# ========== 전체 통합 테스트 ==========

class TestFullValidation:
    def test_all_checks_pass_with_valid_data(self):
        events = make_session_events()
        validator = EventQualityValidator(events)
        results = validator.run_all_checks()
        assert all(r.status == "PASS" for r in results), \
            f"실패한 검증: {[r.check_name for r in results if r.status != 'PASS']}"

    def test_overall_status_fail_when_any_fails(self):
        events = [make_event("page_view", event_id=None)]
        validator = EventQualityValidator(events)
        validator.run_all_checks()
        report = validator.get_summary_report()
        assert report["overall_status"] == "FAIL"

    def test_summary_report_structure(self):
        events = [make_event("page_view")]
        validator = EventQualityValidator(events)
        validator.run_all_checks()
        report = validator.get_summary_report()

        assert "overall_status" in report
        assert "total_checks" in report
        assert "passed" in report
        assert "failed" in report
        assert "checks" in report
        assert report["total_checks"] == 7  # 7가지 검증
        assert isinstance(report["checks"], list)

    def test_check_result_dict_structure(self):
        events = [make_event("page_view")]
        validator = EventQualityValidator(events)
        validator.check_null_fields()
        result_dict = validator.results[0].to_dict()

        expected_keys = {"check_name", "check_type", "target_table",
                         "total_records", "failed_records", "pass_rate",
                         "status", "detail"}
        assert set(result_dict.keys()) == expected_keys


class TestValidateJsonlFile:
    """JSONL 파일 기반 검증 통합 테스트"""

    def test_validate_jsonl_file(self):
        events = make_session_events()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as f:
            for event in events:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
            filepath = f.name

        try:
            report = validate_jsonl_file(filepath)
            assert report["overall_status"] == "PASS"
            assert report["total_checks"] == 7
        finally:
            os.unlink(filepath)
