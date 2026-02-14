"""
이벤트 생성기 테스트
====================================
- 이벤트 구조 유효성
- 플랫폼-디바이스 정합성
- 전환 퍼널 순서
- 시간대 분포 및 날짜 일관성
- 상품 카탈로그 정합성
"""

import pytest
import json
import os
import tempfile
from collections import Counter

from scripts.generate_events import (
    EventGenerator, PRODUCTS, PLATFORM_DEVICE_MAP, FUNNEL_PROBABILITIES,
)


class TestEventStructure:
    """이벤트 기본 구조 검증"""

    def test_events_not_empty(self, events):
        assert len(events) > 0, "이벤트가 생성되지 않았습니다"

    def test_all_events_have_required_fields(self, events):
        required = ["event_id", "event_type", "user_id", "session_id",
                     "timestamp", "platform", "device_type"]
        for event in events:
            for field in required:
                assert field in event, f"필수 필드 누락: {field}"
                assert event[field] is not None, f"필수 필드 NULL: {field}"

    def test_event_types_valid(self, events):
        valid_types = {"page_view", "click", "add_to_cart", "purchase", "search"}
        for event in events:
            assert event["event_type"] in valid_types, \
                f"유효하지 않은 이벤트 유형: {event['event_type']}"

    def test_all_five_event_types_present(self):
        """충분한 사용자 수에서 5가지 이벤트 유형이 모두 생성되는지 확인"""
        gen = EventGenerator(target_date="2026-01-15", num_users=300)
        gen.generate()
        types = {e["event_type"] for e in gen.events}
        expected = {"page_view", "click", "add_to_cart", "purchase", "search"}
        assert types == expected, f"누락된 이벤트 유형: {expected - types}"

    def test_event_id_unique(self, events):
        """모든 event_id가 고유한지 확인"""
        ids = [e["event_id"] for e in events]
        assert len(ids) == len(set(ids)), "중복 event_id 존재"

    def test_events_sorted_by_timestamp(self, events):
        timestamps = [e["timestamp"] for e in events]
        assert timestamps == sorted(timestamps), "이벤트가 시간순 정렬되지 않음"


class TestPlatformDeviceConsistency:
    """플랫폼-디바이스 정합성 검증 (핵심 현실성 테스트)"""

    def test_ios_always_mobile(self, events):
        """iOS 플랫폼은 반드시 mobile 디바이스"""
        ios_events = [e for e in events if e["platform"] == "ios"]
        for e in ios_events:
            assert e["device_type"] == "mobile", \
                f"iOS에서 {e['device_type']} 발생 (event_id: {e['event_id']})"

    def test_android_always_mobile(self, events):
        """Android 플랫폼은 반드시 mobile 디바이스"""
        android_events = [e for e in events if e["platform"] == "android"]
        for e in android_events:
            assert e["device_type"] == "mobile", \
                f"Android에서 {e['device_type']} 발생"

    def test_web_valid_devices(self, events):
        """Web 플랫폼은 desktop/mobile/tablet만 허용"""
        web_events = [e for e in events if e["platform"] == "web"]
        for e in web_events:
            assert e["device_type"] in {"desktop", "mobile", "tablet"}, \
                f"Web에서 유효하지 않은 디바이스: {e['device_type']}"

    def test_platform_device_matches_mapping(self, events):
        """모든 이벤트가 PLATFORM_DEVICE_MAP 규칙을 따르는지 검증"""
        for e in events:
            platform = e["platform"]
            device = e["device_type"]
            allowed = PLATFORM_DEVICE_MAP[platform]["devices"]
            assert device in allowed, \
                f"{platform}+{device} 조합 불가 (허용: {allowed})"

    def test_session_platform_consistency(self, events):
        """동일 세션 내 모든 이벤트는 동일한 platform을 가져야 함"""
        sessions = {}
        for e in events:
            sid = e["session_id"]
            if sid not in sessions:
                sessions[sid] = e["platform"]
            else:
                assert sessions[sid] == e["platform"], \
                    f"세션 {sid[:8]}...에서 플랫폼 변경: {sessions[sid]} → {e['platform']}"

    def test_os_browser_present(self, events):
        """os, browser 필드가 존재하는지 확인"""
        for e in events:
            assert "os" in e and e["os"], f"os 필드 누락"
            assert "browser" in e and e["browser"], f"browser 필드 누락"


class TestFunnelLogic:
    """전환 퍼널 로직 검증"""

    def test_purchase_always_has_prior_cart(self, events):
        """purchase가 있는 세션에는 반드시 add_to_cart가 선행"""
        sessions = {}
        for e in events:
            sid = e["session_id"]
            sessions.setdefault(sid, []).append(e)

        for sid, session_events in sessions.items():
            session_events.sort(key=lambda x: x["timestamp"])
            types = [e["event_type"] for e in session_events]
            if "purchase" in types:
                purchase_idx = types.index("purchase")
                prior_types = set(types[:purchase_idx])
                assert "add_to_cart" in prior_types, \
                    f"세션 {sid[:8]}: purchase 전 add_to_cart 없음"

    def test_cart_always_has_prior_view_or_click(self, events):
        """add_to_cart 전에 page_view 또는 click이 선행"""
        sessions = {}
        for e in events:
            sessions.setdefault(e["session_id"], []).append(e)

        for sid, session_events in sessions.items():
            session_events.sort(key=lambda x: x["timestamp"])
            types = [e["event_type"] for e in session_events]
            if "add_to_cart" in types:
                cart_idx = types.index("add_to_cart")
                prior = set(types[:cart_idx])
                assert prior.intersection({"page_view", "click"}), \
                    f"세션 {sid[:8]}: add_to_cart 전 조회/클릭 없음"

    def test_purchase_has_order_id(self, events):
        purchases = [e for e in events if e["event_type"] == "purchase"]
        for p in purchases:
            assert p.get("order_id"), "purchase에 order_id 누락"

    def test_purchase_has_valid_extra_data(self, events):
        """purchase 이벤트의 extra_data가 유효한 JSON이고 products 포함"""
        purchases = [e for e in events if e["event_type"] == "purchase"]
        for p in purchases:
            assert p.get("extra_data"), "purchase에 extra_data 누락"
            data = json.loads(p["extra_data"])
            assert "products" in data, "extra_data에 products 누락"
            assert len(data["products"]) > 0, "products 배열이 비어있음"

    def test_add_to_cart_has_product_info(self, events):
        carts = [e for e in events if e["event_type"] == "add_to_cart"]
        for c in carts:
            assert c.get("product_id"), "add_to_cart에 product_id 누락"
            assert c.get("quantity") and c["quantity"] > 0, "유효하지 않은 수량"
            assert c.get("unit_price") and c["unit_price"] > 0, "유효하지 않은 가격"


class TestProductCatalog:
    """상품 카탈로그 정합성 검증"""

    def test_product_prices_match_catalog(self, events):
        """생성된 이벤트의 상품 가격이 카탈로그와 일치"""
        for e in events:
            if e.get("product_id") and e.get("unit_price"):
                pid = e["product_id"]
                if pid in PRODUCTS:
                    assert e["unit_price"] == PRODUCTS[pid]["price"], \
                        f"{pid}: 가격 불일치 {e['unit_price']} != {PRODUCTS[pid]['price']}"

    def test_purchase_amount_consistency(self, events):
        """purchase total_amount = Σ(unit_price × quantity) - discount + shipping"""
        purchases = [e for e in events if e["event_type"] == "purchase"]
        for p in purchases:
            data = json.loads(p["extra_data"])
            items_total = sum(i["unit_price"] * i["quantity"] for i in data["products"])
            discount = data.get("discount_amount", 0) or 0
            shipping = data.get("shipping_fee", 0) or 0
            expected = items_total - discount + shipping
            assert abs(p["total_amount"] - expected) <= 1, \
                f"주문 금액 불일치: {p['total_amount']} != {expected}"


class TestDateAndTime:
    """날짜/시간 검증"""

    def test_all_events_match_target_date(self, events):
        """모든 이벤트의 timestamp가 지정 날짜에 해당"""
        for e in events:
            assert e["timestamp"].startswith("2026-01-15"), \
                f"날짜 불일치: {e['timestamp']}"

    def test_hour_distribution_realistic(self, events):
        """시간대 분포가 현실적인지 확인 (새벽 3~4시 최소)"""
        hours = [int(e["timestamp"][11:13]) for e in events]
        hour_counts = Counter(hours)
        # 새벽 3~4시는 전체의 3% 미만이어야 현실적
        late_night = hour_counts.get(3, 0) + hour_counts.get(4, 0)
        assert late_night / len(events) < 0.03, \
            f"새벽 3~4시 비율이 비현실적: {late_night}/{len(events)}"


class TestSaveAndLoad:
    """파일 저장/로딩 검증"""

    def test_save_to_jsonl(self, generator):
        """JSONL 파일 저장 및 재로딩 검증"""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = generator.save_to_json(tmpdir)
            assert os.path.exists(filepath)
            assert filepath.endswith(".jsonl")

            # 파일에서 다시 읽어서 검증
            loaded = []
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        loaded.append(json.loads(line))

            assert len(loaded) == len(generator.events)

    def test_summary_statistics(self, generator):
        summary = generator.get_summary()
        assert summary["total_events"] == len(generator.events)
        assert summary["unique_users"] > 0
        assert summary["unique_sessions"] > 0
        assert summary["date"] == "2026-01-15"
        assert "events_by_type" in summary
        assert "purchase_count" in summary
        assert "total_revenue" in summary


class TestEdgeCases:
    """엣지 케이스 검증"""

    def test_small_user_count(self):
        """소규모 사용자로도 정상 동작"""
        gen = EventGenerator(target_date="2026-01-15", num_users=5)
        events = gen.generate()
        assert len(events) > 0

    def test_large_user_count(self):
        """대규모 사용자도 정상 동작"""
        gen = EventGenerator(target_date="2026-01-15", num_users=1000)
        events = gen.generate()
        assert len(events) > 0

    def test_weekend_has_more_traffic(self):
        """주말이 평일보다 트래픽이 많은 경향"""
        # 여러 번 실행하여 평균 비교 (확률적이므로 느슨하게 검증)
        weekday_counts = []
        weekend_counts = []
        for _ in range(5):
            wed = EventGenerator("2026-01-14", 200)  # 수요일
            wed.generate()
            weekday_counts.append(len(wed.events))

            sat = EventGenerator("2026-01-17", 200)  # 토요일
            sat.generate()
            weekend_counts.append(len(sat.events))

        avg_weekday = sum(weekday_counts) / len(weekday_counts)
        avg_weekend = sum(weekend_counts) / len(weekend_counts)
        # 토요일이 수요일보다 평균적으로 많아야 함 (가중치 1.3 vs 0.9)
        assert avg_weekend > avg_weekday * 0.9, \
            f"주말 트래픽이 비정상적으로 적음: {avg_weekend:.0f} vs {avg_weekday:.0f}"


class TestEdgeCasesAdvanced:
    """심화 엣지 케이스 검증"""

    def test_single_user(self):
        """사용자 1명으로도 크래시 없이 동작"""
        gen = EventGenerator(target_date="2026-01-15", num_users=1)
        events = gen.generate()
        # 1명은 확률적으로 0건일 수 있음 → 크래시 없이 리스트 반환이 핵심
        assert isinstance(events, list)
        summary = gen.get_summary()
        assert summary["total_events"] >= 0

    def test_leap_year_date(self):
        """윤년 2월 29일 날짜 처리"""
        gen = EventGenerator(target_date="2028-02-29", num_users=10)
        events = gen.generate()
        assert len(events) > 0
        assert all(e["timestamp"].startswith("2028-02-29") for e in events)

    def test_year_boundary_date(self):
        """연말/연초 경계 날짜 처리"""
        gen = EventGenerator(target_date="2026-12-31", num_users=10)
        events = gen.generate()
        assert len(events) > 0
        assert all(e["timestamp"].startswith("2026-12-31") for e in events)

    def test_summary_with_no_purchases(self):
        """구매가 0건인 경우에도 summary 정상 반환"""
        gen = EventGenerator(target_date="2026-01-15", num_users=1)
        gen.generate()
        summary = gen.get_summary()
        assert summary["total_revenue"] >= 0
        assert summary["purchase_count"] >= 0

    def test_save_creates_directory(self):
        """저장 경로가 없을 때 디렉터리 자동 생성"""
        gen = EventGenerator(target_date="2026-01-15", num_users=5)
        gen.generate()
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_dir = os.path.join(tmpdir, "deep", "nested", "dir")
            filepath = gen.save_to_json(nested_dir)
            assert os.path.exists(filepath)

    def test_multiple_generate_calls_are_idempotent(self):
        """generate()를 여러 번 호출해도 이벤트가 무한 증가하지 않음"""
        gen = EventGenerator(target_date="2026-01-15", num_users=10)
        gen.generate()
        count1 = len(gen.events)
        gen.generate()
        count2 = len(gen.events)
        # 재생성이므로 크기가 비슷해야 함 (정확히 같을 필요는 없음 - 랜덤성)
        assert abs(count1 - count2) / max(count1, 1) < 0.5

    def test_large_scale_event_id_uniqueness(self):
        """대규모(1000명)에서도 event_id 유니크"""
        gen = EventGenerator(target_date="2026-01-15", num_users=1000)
        gen.generate()
        ids = [e["event_id"] for e in gen.events]
        assert len(ids) == len(set(ids)), f"대규모 생성에서 중복 event_id 존재: {len(ids) - len(set(ids))}건"

    def test_all_platforms_present_in_large_set(self):
        """대규모 생성 시 모든 플랫폼이 존재"""
        gen = EventGenerator(target_date="2026-01-15", num_users=500)
        gen.generate()
        platforms = {e["platform"] for e in gen.events}
        expected = {"web", "ios", "android"}
        assert platforms == expected, f"누락 플랫폼: {expected - platforms}"
