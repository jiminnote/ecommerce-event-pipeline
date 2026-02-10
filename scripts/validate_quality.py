"""
데이터 품질 검증 모듈
====================================
수집된 이벤트 로그의 품질을 다차원으로 검증합니다.

검증 항목:
  1. NULL 체크: 필수 필드 누락 검사
  2. 중복 체크: event_id 중복 검사
  3. 범위 체크: 수치형 필드 유효 범위 검사
  4. 이벤트 순서 검증: 전환 퍼널 순서 정합성 검사
  5. 정합성 체크: 상호 참조 데이터 간 일관성 검사
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    """품질 검증 결과"""
    check_name: str
    check_type: str
    target_table: str
    total_records: int
    failed_records: int
    status: str = ""
    detail: str = ""

    def __post_init__(self):
        self.pass_rate = round(
            (1 - self.failed_records / max(self.total_records, 1)) * 100, 2
        )
        self.status = "PASS" if self.pass_rate >= 99.0 else "FAIL"

    def to_dict(self) -> Dict:
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "target_table": self.target_table,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "pass_rate": self.pass_rate,
            "status": self.status,
            "detail": self.detail,
        }


class EventQualityValidator:
    """이벤트 로그 품질 검증기"""

    REQUIRED_FIELDS = {
        "all": ["event_id", "event_type", "user_id", "session_id", "timestamp"],
        "page_view": ["page_url", "page_type"],
        "click": ["page_url", "element_id", "element_type"],
        "add_to_cart": ["product_id", "quantity", "unit_price"],
        "purchase": ["order_id", "total_amount", "payment_method"],
        "search": ["search_query", "result_count"],
    }

    VALID_EVENT_TYPES = {"page_view", "click", "add_to_cart", "purchase", "search"}
    VALID_PLATFORMS = {"web", "ios", "android"}
    VALID_PAYMENT_METHODS = {"credit_card", "bank_transfer", "kakao_pay", "naver_pay", "toss_pay"}

    # 플랫폼-디바이스 정합성 규칙 (iOS/Android → mobile 고정)
    VALID_PLATFORM_DEVICES = {
        "ios":     {"mobile"},
        "android": {"mobile"},
        "web":     {"desktop", "mobile", "tablet"},
    }

    def __init__(self, events: List[Dict]):
        self.events = events
        self.results: List[QualityCheckResult] = []

    def check_null_fields(self) -> QualityCheckResult:
        """필수 필드 NULL 검사"""
        failed = 0
        failed_details = []

        for i, event in enumerate(self.events):
            # 공통 필수 필드 검사
            for field_name in self.REQUIRED_FIELDS["all"]:
                if not event.get(field_name):
                    failed += 1
                    failed_details.append(f"row {i}: missing '{field_name}'")

            # 이벤트 유형별 필수 필드 검사
            event_type = event.get("event_type", "")
            if event_type in self.REQUIRED_FIELDS:
                for field_name in self.REQUIRED_FIELDS[event_type]:
                    if event.get(field_name) is None:
                        failed += 1
                        failed_details.append(f"row {i}: {event_type} missing '{field_name}'")

        result = QualityCheckResult(
            check_name="필수 필드 NULL 검사",
            check_type="null_check",
            target_table="raw_events",
            total_records=len(self.events),
            failed_records=failed,
            detail="; ".join(failed_details[:10]) + (f" ... (+{len(failed_details)-10}건)" if len(failed_details) > 10 else ""),
        )
        self.results.append(result)
        return result

    def check_duplicates(self) -> QualityCheckResult:
        """event_id 중복 검사"""
        event_ids = [e.get("event_id") for e in self.events]
        duplicates = len(event_ids) - len(set(event_ids))

        result = QualityCheckResult(
            check_name="event_id 중복 검사",
            check_type="duplicate_check",
            target_table="raw_events",
            total_records=len(self.events),
            failed_records=duplicates,
            detail=f"중복 event_id: {duplicates}건",
        )
        self.results.append(result)
        return result

    def check_value_ranges(self) -> QualityCheckResult:
        """수치형 필드 유효 범위 검사"""
        failed = 0
        failed_details = []

        for i, event in enumerate(self.events):
            event_type = event.get("event_type")

            # 수량 범위 (1~99)
            if event_type == "add_to_cart":
                qty = event.get("quantity", 0)
                if not (1 <= qty <= 99):
                    failed += 1
                    failed_details.append(f"row {i}: quantity={qty} out of range")

            # 금액 범위 (양수, 1천만원 이하)
            if event_type == "add_to_cart":
                price = event.get("unit_price", 0)
                if not (0 < price <= 10_000_000):
                    failed += 1
                    failed_details.append(f"row {i}: unit_price={price} out of range")

            if event_type == "purchase":
                amount = event.get("total_amount", 0)
                if not (0 < amount <= 100_000_000):
                    failed += 1
                    failed_details.append(f"row {i}: total_amount={amount} out of range")

            # 검색 결과 수 (0 이상)
            if event_type == "search":
                count = event.get("result_count", -1)
                if count < 0:
                    failed += 1
                    failed_details.append(f"row {i}: result_count={count} negative")

            # event_type 유효성
            if event_type not in self.VALID_EVENT_TYPES:
                failed += 1
                failed_details.append(f"row {i}: invalid event_type='{event_type}'")

        result = QualityCheckResult(
            check_name="필드 값 범위 검사",
            check_type="range_check",
            target_table="raw_events",
            total_records=len(self.events),
            failed_records=failed,
            detail="; ".join(failed_details[:10]),
        )
        self.results.append(result)
        return result

    def check_event_sequence(self) -> QualityCheckResult:
        """전환 퍼널 이벤트 순서 검증
        - purchase 전에 반드시 add_to_cart가 있어야 함
        - add_to_cart 전에 반드시 click/page_view가 있어야 함
        """
        failed = 0
        failed_details = []

        # 세션별로 그룹화
        sessions = {}
        for event in self.events:
            sid = event.get("session_id")
            if sid not in sessions:
                sessions[sid] = []
            sessions[sid].append(event)

        for sid, events in sessions.items():
            events.sort(key=lambda x: x.get("timestamp", ""))
            event_types_in_order = [e.get("event_type") for e in events]

            # purchase가 있는데 add_to_cart가 없는 경우
            if "purchase" in event_types_in_order and "add_to_cart" not in event_types_in_order:
                failed += 1
                failed_details.append(f"session {sid[:8]}...: purchase without add_to_cart")

            # add_to_cart가 있는데 그 전에 page_view/click이 없는 경우
            if "add_to_cart" in event_types_in_order:
                cart_idx = event_types_in_order.index("add_to_cart")
                prior_types = set(event_types_in_order[:cart_idx])
                if not prior_types.intersection({"page_view", "click"}):
                    failed += 1
                    failed_details.append(f"session {sid[:8]}...: add_to_cart without prior view/click")

        result = QualityCheckResult(
            check_name="전환 퍼널 순서 검증",
            check_type="sequence_check",
            target_table="raw_events",
            total_records=len(sessions),
            failed_records=failed,
            detail="; ".join(failed_details[:10]),
        )
        self.results.append(result)
        return result

    def check_timestamp_validity(self) -> QualityCheckResult:
        """타임스탬프 유효성 검사"""
        failed = 0

        for i, event in enumerate(self.events):
            ts = event.get("timestamp")
            try:
                dt = datetime.fromisoformat(ts) if ts else None
                if dt is None:
                    failed += 1
                elif dt.year < 2020 or dt.year > 2030:
                    failed += 1
            except (ValueError, TypeError):
                failed += 1

        result = QualityCheckResult(
            check_name="타임스탬프 유효성 검사",
            check_type="range_check",
            target_table="raw_events",
            total_records=len(self.events),
            failed_records=failed,
            detail=f"유효하지 않은 타임스탬프: {failed}건",
        )
        self.results.append(result)
        return result

    def check_cross_reference(self) -> QualityCheckResult:
        """상호 참조 정합성 검사
        - purchase의 total_amount와 cart items 합계 일치 여부
        """
        failed = 0
        purchase_events = [e for e in self.events if e.get("event_type") == "purchase"]

        for event in purchase_events:
            total = event.get("total_amount", 0)
            extra = event.get("extra_data")
            if extra:
                try:
                    data = json.loads(extra) if isinstance(extra, str) else extra
                    products = data.get("products", [])
                    items_total = sum(p["unit_price"] * p["quantity"] for p in products)
                    discount = data.get("discount_amount", 0) or 0
                    shipping = data.get("shipping_fee", 0) or 0
                    expected = items_total - discount + shipping

                    if abs(total - expected) > 1:  # 1원 오차 허용
                        failed += 1
                except (json.JSONDecodeError, KeyError, TypeError):
                    failed += 1

        result = QualityCheckResult(
            check_name="주문 금액 정합성 검사",
            check_type="integrity_check",
            target_table="raw_events",
            total_records=len(purchase_events),
            failed_records=failed,
            detail=f"금액 불일치: {failed}건 / {len(purchase_events)}건",
        )
        self.results.append(result)
        return result

    def check_platform_device_consistency(self) -> QualityCheckResult:
        """플랫폼-디바이스 정합성 검사
        - iOS/Android → 반드시 mobile
        - web → desktop/mobile/tablet
        - 동일 세션 내 플랫폼 변경 불가
        """
        failed = 0
        failed_details = []

        # 1) 플랫폼-디바이스 조합 검증
        for i, event in enumerate(self.events):
            platform = event.get("platform", "")
            device = event.get("device_type", "")
            if platform in self.VALID_PLATFORM_DEVICES:
                if device not in self.VALID_PLATFORM_DEVICES[platform]:
                    failed += 1
                    failed_details.append(
                        f"row {i}: {platform}+{device} 조합 불가"
                    )

        # 2) 동일 세션 내 플랫폼 일관성 검증
        sessions: Dict = {}
        for event in self.events:
            sid = event.get("session_id")
            platform = event.get("platform")
            if sid not in sessions:
                sessions[sid] = platform
            elif sessions[sid] != platform:
                failed += 1
                failed_details.append(
                    f"session {str(sid)[:8]}...: 플랫폼 변경 {sessions[sid]}→{platform}"
                )

        result = QualityCheckResult(
            check_name="플랫폼-디바이스 정합성 검사",
            check_type="consistency_check",
            target_table="raw_events",
            total_records=len(self.events),
            failed_records=failed,
            detail="; ".join(failed_details[:10]),
        )
        self.results.append(result)
        return result

    def run_all_checks(self) -> List[QualityCheckResult]:
        """전체 품질 검증 실행"""
        logger.info(f"=== 데이터 품질 검증 시작 (총 {len(self.events):,}건) ===")

        checks = [
            self.check_null_fields,
            self.check_duplicates,
            self.check_value_ranges,
            self.check_event_sequence,
            self.check_timestamp_validity,
            self.check_cross_reference,
            self.check_platform_device_consistency,
        ]

        for check_fn in checks:
            result = check_fn()
            status_icon = "[PASS]" if result.status == "PASS" else "[FAIL]"
            logger.info(f"  {status_icon} {result.check_name}: "
                        f"{result.pass_rate}% (실패 {result.failed_records}건)")

        total_pass = sum(1 for r in self.results if r.status == "PASS")
        total_checks = len(self.results)
        logger.info(f"\n=== 검증 완료: {total_pass}/{total_checks} 통과 ===")

        return self.results

    def get_summary_report(self) -> Dict:
        """검증 결과 요약 리포트"""
        return {
            "check_date": datetime.now().isoformat(),
            "total_events": len(self.events),
            "total_checks": len(self.results),
            "passed": sum(1 for r in self.results if r.status == "PASS"),
            "failed": sum(1 for r in self.results if r.status == "FAIL"),
            "overall_status": "PASS" if all(r.status == "PASS" for r in self.results) else "FAIL",
            "checks": [r.to_dict() for r in self.results],
        }


def validate_jsonl_file(filepath: str) -> Dict:
    """JSONL 파일을 읽어서 품질 검증 실행"""
    events = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))

    validator = EventQualityValidator(events)
    validator.run_all_checks()
    return validator.get_summary_report()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        report = validate_jsonl_file(sys.argv[1])
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print("Usage: python validate_quality.py <events_file.jsonl>")
