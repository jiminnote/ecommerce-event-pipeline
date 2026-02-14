"""
테스트 공용 Fixture
====================================
- generator / events: 기본 이벤트 생성기 및 이벤트 리스트
- sample_event_*: 이벤트 유형별 샘플 팩토리
- tmp_jsonl_file: 임시 JSONL 파일 생성/정리
- empty_jsonl_file: 빈 JSONL 파일
- large_events: 대용량 이벤트
"""

import json
import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.generate_events import EventGenerator
from scripts.validate_quality import EventQualityValidator


# ========== 이벤트 생성기 Fixture ==========

@pytest.fixture(scope="module")
def generator():
    """100명 기준 이벤트 생성 (테스트용)"""
    gen = EventGenerator(target_date="2026-01-15", num_users=100)
    gen.generate()
    return gen


@pytest.fixture(scope="module")
def events(generator):
    """생성된 이벤트 리스트"""
    return generator.events


# ========== 이벤트 팩토리 Fixture ==========

@pytest.fixture
def make_sample_event():
    """테스트용 이벤트 생성 팩토리 (함수형 fixture)"""
    def _make(event_type="page_view", **overrides):
        base = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "user_id": "U000001",
            "session_id": str(uuid.uuid4()),
            "timestamp": "2026-01-15T10:30:00",
            "platform": "web",
            "device_type": "desktop",
            "os": "macOS",
            "browser": "Chrome",
        }
        type_fields = {
            "page_view": {"page_url": "https://shop.example.com/home", "page_type": "home"},
            "click": {"page_url": "https://shop.example.com/product/P1001",
                      "element_id": "btn_add", "element_type": "button",
                      "product_id": "P1001", "category_id": "CAT001"},
            "add_to_cart": {"product_id": "P1001", "category_id": "CAT001",
                            "quantity": 1, "unit_price": 89000},
            "purchase": {"order_id": f"ORD{uuid.uuid4().hex[:12]}",
                         "total_amount": 89000, "payment_method": "toss_pay",
                         "extra_data": json.dumps({
                             "products": [{"product_id": "P1001", "quantity": 1, "unit_price": 89000}],
                             "discount_amount": 0, "shipping_fee": 0})},
            "search": {"search_query": "이어폰", "result_count": 15,
                       "page_url": "https://shop.example.com/search"},
        }
        base.update(type_fields.get(event_type, {}))
        base.update(overrides)
        return base
    return _make


@pytest.fixture
def valid_session_events(make_sample_event):
    """정상적인 전환 퍼널 세션 이벤트 세트"""
    sid = str(uuid.uuid4())
    return [
        make_sample_event("page_view",   session_id=sid, timestamp="2026-01-15T10:00:00"),
        make_sample_event("click",       session_id=sid, timestamp="2026-01-15T10:01:00"),
        make_sample_event("add_to_cart", session_id=sid, timestamp="2026-01-15T10:02:00"),
        make_sample_event("purchase",    session_id=sid, timestamp="2026-01-15T10:03:00"),
        make_sample_event("search",      timestamp="2026-01-15T10:04:00"),
    ]


# ========== 파일 기반 Fixture ==========

@pytest.fixture
def tmp_jsonl_file(valid_session_events):
    """임시 JSONL 파일 (정상 데이터)"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        for event in valid_session_events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        filepath = f.name
    yield filepath
    os.unlink(filepath)


@pytest.fixture
def empty_jsonl_file():
    """빈 JSONL 파일"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        filepath = f.name
    yield filepath
    os.unlink(filepath)


@pytest.fixture
def malformed_jsonl_file():
    """잘못된 JSON 형식이 포함된 JSONL 파일"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as f:
        f.write('{"event_id": "valid-1", "event_type": "page_view"}\n')
        f.write('NOT VALID JSON LINE\n')
        f.write('{"event_id": "valid-2",,, broken}\n')
        filepath = f.name
    yield filepath
    os.unlink(filepath)


# ========== 대용량 데이터 Fixture ==========

@pytest.fixture(scope="module")
def large_generator():
    """대용량 이벤트 생성 (1000명)"""
    gen = EventGenerator(target_date="2026-01-15", num_users=1000)
    gen.generate()
    return gen


@pytest.fixture(scope="module")
def large_events(large_generator):
    """대용량 이벤트 리스트"""
    return large_generator.events
