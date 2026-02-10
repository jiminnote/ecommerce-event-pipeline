import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.generate_events import EventGenerator


@pytest.fixture(scope="module")
def generator():
    """100명 기준 이벤트 생성 (테스트용)"""
    gen = EventGenerator(target_date="2026-01-15", num_users=100)
    gen.generate()
    return gen


@pytest.fixture(scope="module")
def events(generator):
    return generator.events
