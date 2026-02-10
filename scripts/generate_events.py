"""
이커머스 사용자 행동 로그 생성기
====================================
가상의 이커머스 사이트에서 발생하는 사용자 행동 이벤트를 생성합니다.
이벤트 스키마(schemas/event_schema.json)를 기반으로 현실적인 행동 패턴을 시뮬레이션합니다.

생성 이벤트 유형:
  - page_view: 페이지 조회
  - click: 요소 클릭
  - add_to_cart: 장바구니 담기
  - purchase: 주문 완료
  - search: 검색

사용법:
  python generate_events.py --date 2026-02-01 --users 500 --output data/events/
  python generate_events.py --start-date 2026-01-01 --end-date 2026-01-31 --users 500
"""

import json
import uuid
import random
import argparse
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional


# ========== 설정 ==========

CATEGORIES = {
    "CAT001": {"name": "가전/디지털", "products": ["P1001", "P1002", "P1003", "P1004", "P1005"]},
    "CAT002": {"name": "패션/의류", "products": ["P2001", "P2002", "P2003", "P2004", "P2005"]},
    "CAT003": {"name": "식품/건강", "products": ["P3001", "P3002", "P3003", "P3004"]},
    "CAT004": {"name": "생활/주방", "products": ["P4001", "P4002", "P4003", "P4004"]},
    "CAT005": {"name": "뷰티/화장품", "products": ["P5001", "P5002", "P5003"]},
}

PRODUCTS = {
    "P1001": {"name": "무선 이어폰", "price": 89000, "category": "CAT001"},
    "P1002": {"name": "노트북 거치대", "price": 35000, "category": "CAT001"},
    "P1003": {"name": "보조배터리", "price": 29000, "category": "CAT001"},
    "P1004": {"name": "무선 충전기", "price": 25000, "category": "CAT001"},
    "P1005": {"name": "블루투스 스피커", "price": 65000, "category": "CAT001"},
    "P2001": {"name": "후드 집업", "price": 49000, "category": "CAT002"},
    "P2002": {"name": "운동화", "price": 79000, "category": "CAT002"},
    "P2003": {"name": "패딩 점퍼", "price": 129000, "category": "CAT002"},
    "P2004": {"name": "청바지", "price": 59000, "category": "CAT002"},
    "P2005": {"name": "맨투맨", "price": 35000, "category": "CAT002"},
    "P3001": {"name": "프로틴 파우더", "price": 45000, "category": "CAT003"},
    "P3002": {"name": "견과류 세트", "price": 22000, "category": "CAT003"},
    "P3003": {"name": "비타민C", "price": 18000, "category": "CAT003"},
    "P3004": {"name": "오메가3", "price": 32000, "category": "CAT003"},
    "P4001": {"name": "텀블러", "price": 28000, "category": "CAT004"},
    "P4002": {"name": "에어프라이어", "price": 89000, "category": "CAT004"},
    "P4003": {"name": "무선 청소기", "price": 199000, "category": "CAT004"},
    "P4004": {"name": "가습기", "price": 55000, "category": "CAT004"},
    "P5001": {"name": "선크림", "price": 22000, "category": "CAT005"},
    "P5002": {"name": "클렌징폼", "price": 15000, "category": "CAT005"},
    "P5003": {"name": "마스크팩 세트", "price": 25000, "category": "CAT005"},
}

PAGE_TYPES = ["home", "category", "product", "cart", "checkout", "order_complete", "mypage"]
PLATFORMS = ["web", "ios", "android"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
ELEMENT_TYPES = ["button", "link", "banner", "product_card", "search_result", "recommendation"]
PAYMENT_METHODS = ["credit_card", "bank_transfer", "kakao_pay", "naver_pay", "toss_pay"]
SEARCH_QUERIES = [
    "이어폰", "노트북", "운동화", "패딩", "비타민", "텀블러", "에어프라이어",
    "선물 추천", "겨울 옷", "충전기", "청소기", "건강식품", "화장품 세트",
    "프로틴", "가습기", "블루투스", "청바지", "맨투맨", "집업"
]

# 플랫폼-디바이스 정합성 매핑 (iOS/Android → mobile 고정)
PLATFORM_DEVICE_MAP = {
    "ios":     {"devices": ["mobile"],                    "os": ["iOS"],     "browsers": ["Safari", "Chrome"]},
    "android": {"devices": ["mobile"],                    "os": ["Android"], "browsers": ["Chrome", "Samsung Internet"]},
    "web":     {"devices": ["desktop", "mobile", "tablet"], "os": ["Windows", "macOS", "Linux"], "browsers": ["Chrome", "Safari", "Firefox", "Edge"]},
}

# 요일별 트래픽 가중치 (0=월 ~ 6=일, 주말 트래픽 증가)
WEEKDAY_MULTIPLIERS = {0: 0.80, 1: 0.85, 2: 0.90, 3: 0.90, 4: 1.00, 5: 1.30, 6: 1.20}

# 전환 퍼널 확률 (현실적인 이커머스 전환율 반영)
FUNNEL_PROBABILITIES = {
    "view_to_click": 0.35,      # 페이지뷰 → 클릭
    "click_to_search": 0.20,    # 클릭 → 검색
    "click_to_cart": 0.12,      # 클릭 → 장바구니
    "cart_to_purchase": 0.45,   # 장바구니 → 구매
}


class EventGenerator:
    """이커머스 사용자 행동 이벤트 생성기"""

    def __init__(self, target_date: str, num_users: int = 500):
        self.target_date = datetime.strptime(target_date, "%Y-%m-%d")
        self.num_users = num_users
        self.user_ids = [f"U{str(i).zfill(6)}" for i in range(1, num_users + 1)]
        self.events: List[Dict] = []
        self._create_user_profiles()

    def _create_user_profiles(self):
        """사용자별 고정 프로파일 생성 (플랫폼·디바이스 일관성 보장)

        실제 서비스에서 한 사용자는 주로 동일한 디바이스를 사용합니다.
        iOS 사용자가 갑자기 desktop으로 잡히면 데이터 신뢰도가 떨어지므로
        사용자별 프로파일을 고정해 현실적인 로그를 생성합니다.
        """
        self.user_profiles = {}
        platforms = random.choices(
            ["web", "ios", "android"],
            weights=[50, 25, 25],
            k=self.num_users
        )
        for uid, platform in zip(self.user_ids, platforms):
            config = PLATFORM_DEVICE_MAP[platform]
            self.user_profiles[uid] = {
                "platform": platform,
                "device_type": random.choice(config["devices"]),
                "os": random.choice(config["os"]),
                "browser": random.choice(config["browsers"]),
            }

    def _generate_timestamp(self, base_time: datetime, offset_minutes: int = 0) -> str:
        """시간대별 가중치를 적용한 타임스탬프 생성 (날짜 범위 보장)"""
        ts = base_time + timedelta(minutes=offset_minutes + random.randint(0, 5))
        # 대상 날짜를 넘기지 않도록 캡
        end_of_day = self.target_date.replace(hour=23, minute=59, second=59)
        if ts > end_of_day:
            ts = end_of_day
        return ts.isoformat()

    def _get_session_start_hour(self) -> int:
        """현실적인 시간대 분포 (출퇴근 + 점심 + 저녁 피크)"""
        hour_weights = {
            0: 2, 1: 1, 2: 1, 3: 0, 4: 0, 5: 1, 6: 2, 7: 4,
            8: 6, 9: 8, 10: 10, 11: 9, 12: 12, 13: 11, 14: 8,
            15: 7, 16: 6, 17: 7, 18: 9, 19: 12, 20: 15, 21: 14,
            22: 10, 23: 5
        }
        hours = list(hour_weights.keys())
        weights = list(hour_weights.values())
        return random.choices(hours, weights=weights, k=1)[0]

    def _create_base_event(self, user_id: str, session_id: str,
                           timestamp: str, event_type: str) -> Dict:
        """공통 이벤트 필드 생성 (사용자 프로파일 기반 일관성 보장)"""
        profile = self.user_profiles[user_id]
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": timestamp,
            "platform": profile["platform"],
            "device_type": profile["device_type"],
            "os": profile["os"],
            "browser": profile["browser"],
        }

    def _generate_page_view(self, user_id: str, session_id: str,
                            timestamp: str, page_type: str = None) -> Dict:
        """page_view 이벤트 생성"""
        event = self._create_base_event(user_id, session_id, timestamp, "page_view")
        pt = page_type or random.choice(PAGE_TYPES)
        event.update({
            "page_url": f"https://shop.example.com/{pt}",
            "page_type": pt,
            "referrer": random.choice([None, "https://google.com", "https://naver.com", "direct"]),
        })
        return event

    def _generate_click(self, user_id: str, session_id: str,
                        timestamp: str, product_id: str = None) -> Dict:
        """click 이벤트 생성"""
        event = self._create_base_event(user_id, session_id, timestamp, "click")
        pid = product_id or random.choice(list(PRODUCTS.keys()))
        event.update({
            "page_url": f"https://shop.example.com/product/{pid}",
            "element_id": f"elem_{random.randint(1, 100)}",
            "element_type": random.choice(ELEMENT_TYPES),
            "product_id": pid,
            "category_id": PRODUCTS[pid]["category"],
        })
        return event

    def _generate_add_to_cart(self, user_id: str, session_id: str,
                              timestamp: str, product_id: str) -> Dict:
        """add_to_cart 이벤트 생성"""
        event = self._create_base_event(user_id, session_id, timestamp, "add_to_cart")
        event.update({
            "product_id": product_id,
            "category_id": PRODUCTS[product_id]["category"],
            "quantity": random.choices([1, 2, 3], weights=[70, 20, 10], k=1)[0],
            "unit_price": PRODUCTS[product_id]["price"],
        })
        return event

    def _generate_purchase(self, user_id: str, session_id: str,
                           timestamp: str, cart_items: List[Dict]) -> Dict:
        """purchase 이벤트 생성"""
        event = self._create_base_event(user_id, session_id, timestamp, "purchase")
        total = sum(item["unit_price"] * item["quantity"] for item in cart_items)
        discount = random.choice([0, 0, 0, 1000, 2000, 3000, 5000])
        shipping = 0 if total >= 50000 else 3000

        event.update({
            "order_id": f"ORD{self.target_date.strftime('%Y%m%d')}{random.randint(10000, 99999)}",
            "total_amount": total - discount + shipping,
            "payment_method": random.choice(PAYMENT_METHODS),
            "extra_data": json.dumps({
                "products": [{"product_id": i["product_id"], "quantity": i["quantity"],
                              "unit_price": i["unit_price"]} for i in cart_items],
                "discount_amount": discount,
                "shipping_fee": shipping,
                "coupon_code": f"COUP{random.randint(100, 999)}" if discount > 0 else None,
            }),
        })
        return event

    def _generate_search(self, user_id: str, session_id: str, timestamp: str) -> Dict:
        """search 이벤트 생성"""
        event = self._create_base_event(user_id, session_id, timestamp, "search")
        event.update({
            "search_query": random.choice(SEARCH_QUERIES),
            "result_count": random.randint(0, 150),
            "page_url": "https://shop.example.com/search",
        })
        return event

    def _simulate_user_session(self, user_id: str) -> List[Dict]:
        """한 사용자의 세션을 시뮬레이션 (전환 퍼널 기반)"""
        session_events = []
        session_id = str(uuid.uuid4())
        hour = self._get_session_start_hour()
        base_time = self.target_date.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        elapsed = 0

        # Step 1: 페이지 조회 (항상 발생)
        num_page_views = random.randint(1, 6)
        for i in range(num_page_views):
            elapsed += random.randint(1, 8)
            ev = self._generate_page_view(user_id, session_id,
                                          self._generate_timestamp(base_time, elapsed))
            session_events.append(ev)

        # Step 2: 클릭 (확률 기반)
        if random.random() < FUNNEL_PROBABILITIES["view_to_click"]:
            num_clicks = random.randint(1, 4)
            clicked_products = []
            for _ in range(num_clicks):
                elapsed += random.randint(1, 5)
                pid = random.choice(list(PRODUCTS.keys()))
                clicked_products.append(pid)
                ev = self._generate_click(user_id, session_id,
                                          self._generate_timestamp(base_time, elapsed), pid)
                session_events.append(ev)

            # 검색 (확률 기반)
            if random.random() < FUNNEL_PROBABILITIES["click_to_search"]:
                elapsed += random.randint(1, 3)
                ev = self._generate_search(user_id, session_id,
                                           self._generate_timestamp(base_time, elapsed))
                session_events.append(ev)

            # Step 3: 장바구니 담기 (확률 기반)
            cart_items = []
            if random.random() < FUNNEL_PROBABILITIES["click_to_cart"]:
                num_cart = random.randint(1, min(3, len(clicked_products)))
                for pid in random.sample(clicked_products, num_cart):
                    elapsed += random.randint(1, 3)
                    ev = self._generate_add_to_cart(user_id, session_id,
                                                    self._generate_timestamp(base_time, elapsed), pid)
                    session_events.append(ev)
                    cart_items.append({
                        "product_id": pid,
                        "quantity": ev["quantity"],
                        "unit_price": ev["unit_price"]
                    })

                # Step 4: 구매 (확률 기반)
                if cart_items and random.random() < FUNNEL_PROBABILITIES["cart_to_purchase"]:
                    elapsed += random.randint(2, 10)
                    # 체크아웃 페이지뷰
                    ev_checkout = self._generate_page_view(user_id, session_id,
                                                           self._generate_timestamp(base_time, elapsed),
                                                           "checkout")
                    session_events.append(ev_checkout)

                    elapsed += random.randint(1, 5)
                    ev = self._generate_purchase(user_id, session_id,
                                                 self._generate_timestamp(base_time, elapsed),
                                                 cart_items)
                    session_events.append(ev)

                    # 주문완료 페이지뷰
                    elapsed += 1
                    ev_complete = self._generate_page_view(user_id, session_id,
                                                           self._generate_timestamp(base_time, elapsed),
                                                           "order_complete")
                    session_events.append(ev_complete)

        return session_events

    def generate(self) -> List[Dict]:
        """전체 이벤트 생성 (요일별 트래픽 가중치 적용)"""
        self.events = []
        day_mult = WEEKDAY_MULTIPLIERS.get(self.target_date.weekday(), 1.0)
        active_ratio = min(random.uniform(0.4, 0.7) * day_mult, 1.0)
        active_users = random.sample(self.user_ids, k=int(self.num_users * active_ratio))

        for user_id in active_users:
            num_sessions = random.choices([1, 2, 3], weights=[60, 30, 10], k=1)[0]
            for _ in range(num_sessions):
                session_events = self._simulate_user_session(user_id)
                self.events.extend(session_events)

        self.events.sort(key=lambda x: x["timestamp"])
        return self.events

    def save_to_json(self, output_dir: str) -> str:
        """이벤트를 JSON Lines 형식으로 저장"""
        os.makedirs(output_dir, exist_ok=True)
        filename = f"events_{self.target_date.strftime('%Y%m%d')}.jsonl"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            for event in self.events:
                f.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")

        return filepath

    def get_summary(self) -> Dict:
        """생성된 이벤트 요약 통계"""
        from collections import Counter
        type_counts = Counter(e["event_type"] for e in self.events)
        return {
            "date": self.target_date.strftime("%Y-%m-%d"),
            "total_events": len(self.events),
            "unique_users": len(set(e["user_id"] for e in self.events)),
            "unique_sessions": len(set(e["session_id"] for e in self.events)),
            "events_by_type": dict(type_counts),
            "purchase_count": type_counts.get("purchase", 0),
            "total_revenue": sum(
                e.get("total_amount", 0) for e in self.events if e["event_type"] == "purchase"
            ),
        }


def main():
    parser = argparse.ArgumentParser(description="이커머스 행동 로그 생성기")
    parser.add_argument("--date", type=str, help="생성 대상 날짜 (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="시작 날짜 (범위 생성 시)")
    parser.add_argument("--end-date", type=str, help="종료 날짜 (범위 생성 시)")
    parser.add_argument("--users", type=int, default=500, help="전체 사용자 수 (기본: 500)")
    parser.add_argument("--output", type=str, default="data/events", help="출력 디렉토리")
    args = parser.parse_args()

    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
        current = start
        total_events = 0
        while current <= end:
            gen = EventGenerator(current.strftime("%Y-%m-%d"), args.users)
            gen.generate()
            filepath = gen.save_to_json(args.output)
            summary = gen.get_summary()
            total_events += summary["total_events"]
            print(f"[{summary['date']}] {summary['total_events']:,}건 생성 → {filepath}")
            current += timedelta(days=1)
        print(f"\n총 {total_events:,}건 생성 완료")
    else:
        date = args.date or datetime.now().strftime("%Y-%m-%d")
        gen = EventGenerator(date, args.users)
        gen.generate()
        filepath = gen.save_to_json(args.output)
        summary = gen.get_summary()
        print(f"\n=== 이벤트 생성 완료 ===")
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        print(f"\n저장 위치: {filepath}")


if __name__ == "__main__":
    main()
