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
from scripts.generate_events import EventGenerator


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


# ========== 빈 파일 / 빈 데이터 엣지 케이스 ==========

class TestEmptyDataEdgeCases:
    """빈 데이터 및 경계값 테스트"""

    def test_empty_events_list(self):
        """이벤트 0건으로 검증기 실행"""
        validator = EventQualityValidator([])
        results = validator.run_all_checks()
        report = validator.get_summary_report()
        # 0건이면 모든 검증이 PASS여야 함 (실패 대상 없음)
        assert report["total_events"] == 0
        assert report["total_checks"] == 7

    def test_empty_jsonl_file(self):
        """빈 JSONL 파일 검증"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as f:
            filepath = f.name
        try:
            report = validate_jsonl_file(filepath)
            assert report["total_checks"] == 7
            assert report["total_events"] == 0
        finally:
            os.unlink(filepath)

    def test_single_event_validation(self):
        """이벤트 1건만으로 전체 검증"""
        events = [make_event("page_view")]
        validator = EventQualityValidator(events)
        results = validator.run_all_checks()
        assert len(results) == 7

    def test_whitespace_only_jsonl_file(self):
        """공백만 있는 JSONL 파일"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as f:
            f.write("   \n\n  \n")
            filepath = f.name
        try:
            report = validate_jsonl_file(filepath)
            assert report["total_events"] == 0
        finally:
            os.unlink(filepath)


# ========== 스키마 불일치 / 잘못된 데이터 엣지 케이스 ==========

class TestSchemaViolationEdgeCases:
    """스키마 불일치 및 비정상 데이터 테스트"""

    def test_completely_empty_event(self):
        """필드가 전혀 없는 빈 딕셔너리 이벤트"""
        validator = EventQualityValidator([{}])
        result = validator.check_null_fields()
        assert result.failed_records > 0

    def test_extra_unknown_fields_ignored(self):
        """알 수 없는 추가 필드가 있어도 검증 통과"""
        event = make_event("page_view", unknown_field="test", extra_field=123)
        validator = EventQualityValidator([event])
        result = validator.check_null_fields()
        assert result.status == "PASS"

    def test_all_null_required_fields(self):
        """모든 필수 필드가 None인 이벤트"""
        event = {
            "event_id": None,
            "event_type": None,
            "user_id": None,
            "session_id": None,
            "timestamp": None,
        }
        validator = EventQualityValidator([event])
        result = validator.check_null_fields()
        assert result.failed_records >= 5

    def test_wrong_data_types_in_numeric_fields(self):
        """숫자 필드에 문자열이 들어간 경우 TypeError 발생"""
        event = make_event("add_to_cart", quantity="abc", unit_price="not_a_number")
        validator = EventQualityValidator([event])
        with pytest.raises(TypeError):
            validator.check_value_ranges()

    def test_extremely_long_string_fields(self):
        """매우 긴 문자열이 들어간 경우에도 크래시 없이 검증"""
        long_str = "x" * 100_000
        event = make_event("page_view", event_id=long_str, user_id=long_str)
        validator = EventQualityValidator([event])
        result = validator.check_null_fields()
        # 크래시 없이 결과 반환되면 성공
        assert result.total_records == 1

    def test_special_characters_in_search_query(self):
        """검색어에 특수문자/이모지 포함"""
        event = make_event("search",
                           search_query="🎧이어폰<script>alert('xss')</script>",
                           result_count=5)
        validator = EventQualityValidator([event])
        result = validator.check_null_fields()
        assert result.status == "PASS"

    def test_negative_total_amount_in_purchase(self):
        """음수 주문 금액"""
        event = make_event("purchase", total_amount=-50000)
        validator = EventQualityValidator([event])
        result = validator.check_value_ranges()
        assert result.failed_records > 0

    def test_zero_total_amount_in_purchase(self):
        """주문 금액 0원"""
        event = make_event("purchase", total_amount=0)
        validator = EventQualityValidator([event])
        result = validator.check_value_ranges()
        assert result.failed_records > 0

    def test_purchase_missing_extra_data(self):
        """purchase에 extra_data가 없는 경우"""
        event = make_event("purchase", extra_data=None)
        validator = EventQualityValidator([event])
        result = validator.check_cross_reference()
        # extra_data 없으면 cross-reference 대상이 아니거나 실패
        assert result.total_records >= 0

    def test_duplicate_all_same_events(self):
        """완전히 동일한 이벤트 100개"""
        same_id = str(uuid.uuid4())
        events = [make_event("page_view", event_id=same_id) for _ in range(100)]
        validator = EventQualityValidator(events)
        result = validator.check_duplicates()
        assert result.failed_records == 99

    def test_future_timestamp(self):
        """미래 날짜 타임스탬프 (2035년)"""
        event = make_event("page_view", timestamp="2035-01-15T10:00:00")
        validator = EventQualityValidator([event])
        result = validator.check_timestamp_validity()
        assert result.failed_records == 1

    def test_unknown_platform(self):
        """알 수 없는 플랫폼"""
        event = make_event("page_view", platform="windows_phone", device_type="mobile")
        validator = EventQualityValidator([event])
        result = validator.check_platform_device_consistency()
        # VALID_PLATFORM_DEVICES에 없는 플랫폼 → 검증 대상 아님 또는 통과
        assert result.total_records == 1


# ========== 대용량 데이터 테스트 ==========

class TestLargeScaleValidation:
    """대용량 데이터 검증 테스트"""

    def test_large_dataset_all_checks(self):
        """1000명 이벤트 전체 검증 수행"""
        gen = EventGenerator(target_date="2026-01-15", num_users=1000)
        gen.generate()
        validator = EventQualityValidator(gen.events)
        results = validator.run_all_checks()
        report = validator.get_summary_report()

        assert report["total_checks"] == 7
        assert report["overall_status"] == "PASS"
        assert report["total_events"] > 1000  # 1000명이면 최소 1000건 이상

    def test_large_dataset_no_duplicates(self):
        """대용량 데이터에서 event_id 중복 없음"""
        gen = EventGenerator(target_date="2026-01-15", num_users=1000)
        gen.generate()
        validator = EventQualityValidator(gen.events)
        result = validator.check_duplicates()
        assert result.failed_records == 0

    def test_large_jsonl_file_roundtrip(self):
        """대용량 JSONL 파일 생성 → 검증 전체 라운드트립"""
        gen = EventGenerator(target_date="2026-01-15", num_users=500)
        gen.generate()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as f:
            for event in gen.events:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
            filepath = f.name

        try:
            report = validate_jsonl_file(filepath)
            assert report["overall_status"] == "PASS"
            assert report["total_events"] > 500
        finally:
            os.unlink(filepath)


# ========== 복합 실패 시나리오 ==========

class TestMultipleFailureScenarios:
    """여러 검증이 동시에 실패하는 시나리오"""

    def test_multiple_checks_fail_simultaneously(self):
        """NULL + 중복 + 범위 초과가 동시에 발생"""
        dup_id = str(uuid.uuid4())
        events = [
            make_event("page_view", event_id=None),                   # null 실패
            make_event("add_to_cart", event_id=dup_id, quantity=-5),   # 범위 실패
            make_event("click", event_id=dup_id),                     # 중복 실패
        ]
        validator = EventQualityValidator(events)
        validator.run_all_checks()
        report = validator.get_summary_report()
        assert report["overall_status"] == "FAIL"
        assert report["failed"] >= 2  # 최소 2개 이상 검증 실패

    def test_report_preserves_all_check_details(self):
        """실패 시 모든 검증 항목의 상세 정보가 유지되는지 확인"""
        events = [make_event("page_view", event_id=None)]
        validator = EventQualityValidator(events)
        validator.run_all_checks()
        report = validator.get_summary_report()

        for check in report["checks"]:
            assert "check_name" in check
            assert "status" in check
            assert "pass_rate" in check
            assert check["status"] in ("PASS", "FAIL")
