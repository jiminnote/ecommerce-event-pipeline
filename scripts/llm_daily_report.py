"""
LLM 기반 일간 매출 자동 요약 리포트 생성 모듈
====================================
마트 4종(User, Funnel, Product, Order) 집계 데이터를
LLM API(OpenAI GPT / Anthropic Claude)로 분석하여
자연어 비즈니스 리포트를 자동 생성하고 Slack으로 발송합니다.

지원 LLM:
  - OpenAI GPT-4o / GPT-4o-mini (기본)
  - Anthropic Claude 3.5 Sonnet

설정 (환경변수):
  LLM_PROVIDER       : "openai" (기본) 또는 "anthropic"
  OPENAI_API_KEY      : OpenAI API 키
  ANTHROPIC_API_KEY   : Anthropic API 키
  LLM_MODEL           : 모델명 오버라이드 (선택)

사용법:
  from scripts.llm_daily_report import LLMDailyReporter

  reporter = LLMDailyReporter(provider="openai")
  report = reporter.generate_report(mart_data, execution_date="2026-01-15")
  reporter.send_to_slack(report)
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)


# ========== 프롬프트 템플릿 ==========

SYSTEM_PROMPT = """당신은 이커머스 데이터 분석 전문가입니다.
마트 4종(사용자, 전환 퍼널, 상품, 주문)의 일간 집계 데이터를 분석하여
경영진에게 보고할 수 있는 간결하고 인사이트 있는 한국어 비즈니스 리포트를 작성합니다.

리포트 작성 규칙:
1. 핵심 지표(매출, 주문수, 전환율 등)를 먼저 요약합니다.
2. 전일 대비 또는 기준 대비 변화가 있으면 언급합니다.
3. 플랫폼별(web/ios/android) 퍼널 전환율을 비교 분석합니다.
4. 매출 상위 상품과 전환율 우수 상품을 하이라이트합니다.
5. 결제 수단 분포를 분석합니다.
6. 데이터 기반 액션 아이템을 1~3가지 제안합니다.
7. 금액은 원(₩) 단위로, 비율은 %로 표기합니다.
8. 전체 분량은 Slack 메시지에 적합하도록 1000자 내외로 작성합니다.
"""

USER_PROMPT_TEMPLATE = """아래는 {execution_date} 이커머스 일간 마트 집계 데이터입니다.
이 데이터를 분석하여 일간 비즈니스 리포트를 작성해 주세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 사용자 마트 (mart_user_daily) 요약
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{user_mart_summary}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔄 전환 퍼널 마트 (mart_funnel_daily)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{funnel_mart_summary}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 상품 마트 (mart_product_daily) — 매출 TOP 10
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{product_mart_summary}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🛒 주문 마트 (mart_orders) 요약
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{order_mart_summary}
"""


class LLMClient:
    """LLM API 클라이언트 (OpenAI / Anthropic 지원)"""

    OPENAI_URL = "https://api.openai.com/v1/chat/completions"
    ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

    DEFAULT_MODELS = {
        "openai": "gpt-4o-mini",
        "anthropic": "claude-3-5-sonnet-20241022",
    }

    def __init__(
        self,
        provider: str = "openai",
        api_key: str = None,
        model: str = None,
    ):
        self.provider = provider.lower()
        if self.provider not in ("openai", "anthropic"):
            raise ValueError(f"지원하지 않는 LLM provider: {self.provider}. 'openai' 또는 'anthropic'을 사용하세요.")

        self.api_key = api_key or self._get_api_key()
        self.model = model or os.environ.get("LLM_MODEL") or self.DEFAULT_MODELS[self.provider]

    def _get_api_key(self) -> Optional[str]:
        """환경변수에서 API 키 조회"""
        if self.provider == "openai":
            return os.environ.get("OPENAI_API_KEY")
        else:
            return os.environ.get("ANTHROPIC_API_KEY")

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)

    def generate(self, system_prompt: str, user_prompt: str, max_tokens: int = 2000) -> str:
        """LLM API 호출하여 텍스트 생성

        Args:
            system_prompt: 시스템 프롬프트 (역할 정의)
            user_prompt: 사용자 프롬프트 (데이터 + 지시)
            max_tokens: 최대 생성 토큰 수

        Returns:
            생성된 텍스트

        Raises:
            RuntimeError: API 호출 실패 시
        """
        if not self.is_configured:
            logger.warning(
                "[LLMClient] API 키 미설정 — %s_API_KEY 환경변수를 지정하세요. "
                "폴백 리포트를 생성합니다.",
                self.provider.upper(),
            )
            return self._generate_fallback(user_prompt)

        if self.provider == "openai":
            return self._call_openai(system_prompt, user_prompt, max_tokens)
        else:
            return self._call_anthropic(system_prompt, user_prompt, max_tokens)

    def _call_openai(self, system_prompt: str, user_prompt: str, max_tokens: int) -> str:
        """OpenAI Chat Completions API 호출"""
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.3,
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(self.OPENAI_URL, data=data, headers=headers)
            with urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                return result["choices"][0]["message"]["content"]
        except (URLError, KeyError, Exception) as e:
            logger.error("[LLMClient] OpenAI API 호출 실패: %s", str(e))
            raise RuntimeError(f"OpenAI API 호출 실패: {e}") from e

    def _call_anthropic(self, system_prompt: str, user_prompt: str, max_tokens: int) -> str:
        """Anthropic Messages API 호출"""
        payload = {
            "model": self.model,
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": [
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.3,
        }
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(self.ANTHROPIC_URL, data=data, headers=headers)
            with urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                return result["content"][0]["text"]
        except (URLError, KeyError, Exception) as e:
            logger.error("[LLMClient] Anthropic API 호출 실패: %s", str(e))
            raise RuntimeError(f"Anthropic API 호출 실패: {e}") from e

    def _generate_fallback(self, user_prompt: str) -> str:
        """API 키 미설정 시 폴백 리포트 (데이터 요약만 반환)"""
        return (
            "📋 *일간 매출 리포트 (API 키 미설정 — 폴백 모드)*\n\n"
            "LLM API 키가 설정되지 않아 자동 분석 리포트를 생성할 수 없습니다.\n"
            "아래 환경변수 중 하나를 설정해 주세요:\n"
            "• `OPENAI_API_KEY` (OpenAI GPT)\n"
            "• `ANTHROPIC_API_KEY` (Anthropic Claude)\n\n"
            "원본 데이터 요약:\n"
            f"{user_prompt[:1500]}"
        )


class MartDataExtractor:
    """마트 4종에서 리포트용 데이터를 추출하는 유틸리티"""

    @staticmethod
    def extract_from_db(hook, execution_date: str) -> Dict[str, Any]:
        """PostgreSQL 마트 테이블에서 집계 데이터를 조회

        Args:
            hook: PostgresHook 인스턴스
            execution_date: 대상 날짜 (YYYY-MM-DD)

        Returns:
            마트별 집계 데이터 딕셔너리
        """
        conn = hook.get_conn()
        cursor = conn.cursor()
        mart_data = {}

        # 1) 사용자 마트 요약
        cursor.execute("""
            SELECT
                COUNT(DISTINCT user_id)                         AS active_users,
                COALESCE(SUM(session_count), 0)                 AS total_sessions,
                COALESCE(SUM(page_view_count), 0)               AS total_page_views,
                COALESCE(SUM(click_count), 0)                   AS total_clicks,
                COALESCE(SUM(add_to_cart_count), 0)             AS total_add_to_carts,
                COALESCE(SUM(purchase_count), 0)                AS total_purchases,
                COALESCE(SUM(total_purchase_amount), 0)         AS total_revenue,
                ROUND(AVG(total_purchase_amount)
                      FILTER (WHERE purchase_count > 0), 0)     AS avg_purchase_amount,
                COUNT(CASE WHEN purchase_count > 0 THEN 1 END)  AS purchasing_users
            FROM mart_user_daily
            WHERE activity_date = %s
        """, (execution_date,))
        row = cursor.fetchone()
        if row:
            mart_data["user_mart"] = {
                "active_users": row[0],
                "total_sessions": row[1],
                "total_page_views": row[2],
                "total_clicks": row[3],
                "total_add_to_carts": row[4],
                "total_purchases": row[5],
                "total_revenue": float(row[6]),
                "avg_purchase_amount": float(row[7]) if row[7] else 0,
                "purchasing_users": row[8],
            }
        else:
            mart_data["user_mart"] = {}

        # 2) 전환 퍼널 마트 (플랫폼별)
        cursor.execute("""
            SELECT
                platform,
                step1_viewers, step2_clickers, step3_cart_adders, step4_purchasers,
                view_to_click_rate, click_to_cart_rate,
                cart_to_purchase_rate, overall_conversion_rate
            FROM mart_funnel_daily
            WHERE funnel_date = %s
            ORDER BY step1_viewers DESC
        """, (execution_date,))
        funnel_rows = cursor.fetchall()
        mart_data["funnel_mart"] = [
            {
                "platform": r[0],
                "viewers": r[1],
                "clickers": r[2],
                "cart_adders": r[3],
                "purchasers": r[4],
                "view_to_click_rate": float(r[5]) if r[5] else 0,
                "click_to_cart_rate": float(r[6]) if r[6] else 0,
                "cart_to_purchase_rate": float(r[7]) if r[7] else 0,
                "overall_conversion_rate": float(r[8]) if r[8] else 0,
            }
            for r in funnel_rows
        ]

        # 3) 상품 마트 — 매출 TOP 10
        cursor.execute("""
            SELECT
                product_id,
                click_count, cart_add_count, purchase_count,
                revenue, units_sold, unique_viewers, unique_buyers,
                conversion_rate
            FROM mart_product_daily
            WHERE activity_date = %s
            ORDER BY revenue DESC
            LIMIT 10
        """, (execution_date,))
        product_rows = cursor.fetchall()
        mart_data["product_mart"] = [
            {
                "product_id": r[0],
                "click_count": r[1],
                "cart_add_count": r[2],
                "purchase_count": r[3],
                "revenue": float(r[4]),
                "units_sold": r[5],
                "unique_viewers": r[6],
                "unique_buyers": r[7],
                "conversion_rate": float(r[8]) if r[8] else 0,
            }
            for r in product_rows
        ]

        # 4) 주문 마트 요약
        cursor.execute("""
            SELECT
                COUNT(*)                                    AS order_count,
                COALESCE(SUM(total_amount), 0)              AS total_revenue,
                ROUND(AVG(total_amount), 0)                 AS avg_order_value,
                MAX(total_amount)                           AS max_order_amount,
                COUNT(DISTINCT user_id)                     AS unique_buyers,
                COUNT(DISTINCT payment_method)              AS payment_method_count
            FROM mart_orders
            WHERE order_date = %s
        """, (execution_date,))
        order_row = cursor.fetchone()
        if order_row:
            mart_data["order_mart_summary"] = {
                "order_count": order_row[0],
                "total_revenue": float(order_row[1]),
                "avg_order_value": float(order_row[2]) if order_row[2] else 0,
                "max_order_amount": float(order_row[3]) if order_row[3] else 0,
                "unique_buyers": order_row[4],
                "payment_method_count": order_row[5],
            }
        else:
            mart_data["order_mart_summary"] = {}

        # 결제수단 분포
        cursor.execute("""
            SELECT
                payment_method,
                COUNT(*)                        AS order_count,
                COALESCE(SUM(total_amount), 0)  AS total_amount
            FROM mart_orders
            WHERE order_date = %s
            GROUP BY payment_method
            ORDER BY total_amount DESC
        """, (execution_date,))
        payment_rows = cursor.fetchall()
        mart_data["payment_distribution"] = [
            {
                "method": r[0],
                "order_count": r[1],
                "total_amount": float(r[2]),
            }
            for r in payment_rows
        ]

        cursor.close()
        conn.close()
        return mart_data

    @staticmethod
    def extract_from_report(report_path: str) -> Dict[str, Any]:
        """로컬 파이프라인 리포트 JSON에서 데이터 추출 (DB 미사용 시 폴백)

        Args:
            report_path: pipeline_report_{date}.json 파일 경로

        Returns:
            리포트 데이터 딕셔너리
        """
        with open(report_path, "r", encoding="utf-8") as f:
            report = json.load(f)

        events = report.get("events", {})
        return {
            "user_mart": {
                "active_users": events.get("unique_users", 0),
                "total_sessions": events.get("unique_sessions", 0),
                "total_page_views": events.get("event_breakdown", {}).get("page_view", 0),
                "total_clicks": events.get("event_breakdown", {}).get("click", 0),
                "total_add_to_carts": events.get("event_breakdown", {}).get("add_to_cart", 0),
                "total_purchases": events.get("purchase_count", 0),
                "total_revenue": events.get("total_revenue", 0),
                "avg_purchase_amount": (
                    events.get("total_revenue", 0) / max(events.get("purchase_count", 1), 1)
                ),
                "purchasing_users": events.get("purchase_count", 0),
            },
            "funnel_mart": [],
            "product_mart": [],
            "order_mart_summary": {
                "order_count": events.get("purchase_count", 0),
                "total_revenue": events.get("total_revenue", 0),
                "avg_order_value": (
                    events.get("total_revenue", 0) / max(events.get("purchase_count", 1), 1)
                ),
                "max_order_amount": 0,
                "unique_buyers": 0,
                "payment_method_count": 0,
            },
            "payment_distribution": [],
        }


class LLMDailyReporter:
    """LLM 기반 일간 매출 리포트 생성기"""

    def __init__(
        self,
        provider: str = None,
        api_key: str = None,
        model: str = None,
    ):
        """
        Args:
            provider: LLM 프로바이더 ("openai" 또는 "anthropic")
            api_key: API 키 (미지정 시 환경변수)
            model: 모델명 (미지정 시 기본값)
        """
        self.provider = provider or os.environ.get("LLM_PROVIDER", "openai")
        self.llm = LLMClient(provider=self.provider, api_key=api_key, model=model)

    def _format_user_mart(self, data: Dict) -> str:
        """사용자 마트 데이터 → 텍스트"""
        if not data:
            return "데이터 없음"
        return (
            f"• 활성 사용자: {data.get('active_users', 0):,}명\n"
            f"• 총 세션 수: {data.get('total_sessions', 0):,}회\n"
            f"• 페이지뷰: {data.get('total_page_views', 0):,}회\n"
            f"• 클릭: {data.get('total_clicks', 0):,}회\n"
            f"• 장바구니 추가: {data.get('total_add_to_carts', 0):,}회\n"
            f"• 구매 건수: {data.get('total_purchases', 0):,}건\n"
            f"• 총 매출: ₩{data.get('total_revenue', 0):,.0f}\n"
            f"• 구매자 평균 결제 금액: ₩{data.get('avg_purchase_amount', 0):,.0f}\n"
            f"• 구매 사용자 수: {data.get('purchasing_users', 0):,}명"
        )

    def _format_funnel_mart(self, data: List[Dict]) -> str:
        """전환 퍼널 마트 데이터 → 텍스트"""
        if not data:
            return "데이터 없음"
        lines = []
        for row in data:
            lines.append(
                f"[{row['platform'].upper()}] "
                f"조회 {row['viewers']:,} → 클릭 {row['clickers']:,} "
                f"→ 장바구니 {row['cart_adders']:,} → 구매 {row['purchasers']:,}\n"
                f"  전환율: 조회→클릭 {row['view_to_click_rate']:.1f}% | "
                f"클릭→장바구니 {row['click_to_cart_rate']:.1f}% | "
                f"장바구니→구매 {row['cart_to_purchase_rate']:.1f}% | "
                f"전체 {row['overall_conversion_rate']:.1f}%"
            )
        return "\n".join(lines)

    def _format_product_mart(self, data: List[Dict]) -> str:
        """상품 마트 TOP 10 → 텍스트"""
        if not data:
            return "데이터 없음"
        lines = []
        for i, row in enumerate(data, 1):
            lines.append(
                f"{i}. {row['product_id']} — "
                f"매출 ₩{row['revenue']:,.0f} | "
                f"판매 {row['units_sold']}개 | "
                f"전환율 {row['conversion_rate']:.1f}%"
            )
        return "\n".join(lines)

    def _format_order_mart(self, summary: Dict, payments: List[Dict]) -> str:
        """주문 마트 요약 → 텍스트"""
        if not summary:
            return "데이터 없음"
        lines = [
            f"• 총 주문: {summary.get('order_count', 0):,}건",
            f"• 총 매출: ₩{summary.get('total_revenue', 0):,.0f}",
            f"• 평균 주문 금액: ₩{summary.get('avg_order_value', 0):,.0f}",
            f"• 최대 주문 금액: ₩{summary.get('max_order_amount', 0):,.0f}",
            f"• 결제수단 수: {summary.get('payment_method_count', 0)}종",
        ]
        if payments:
            lines.append("\n결제수단별 분포:")
            for p in payments:
                lines.append(
                    f"  • {p['method']}: {p['order_count']}건 / ₩{p['total_amount']:,.0f}"
                )
        return "\n".join(lines)

    def build_prompt(self, mart_data: Dict[str, Any], execution_date: str) -> str:
        """마트 데이터 → LLM 프롬프트 생성

        Args:
            mart_data: MartDataExtractor 로 추출한 데이터
            execution_date: 대상 날짜

        Returns:
            완성된 사용자 프롬프트
        """
        return USER_PROMPT_TEMPLATE.format(
            execution_date=execution_date,
            user_mart_summary=self._format_user_mart(mart_data.get("user_mart", {})),
            funnel_mart_summary=self._format_funnel_mart(mart_data.get("funnel_mart", [])),
            product_mart_summary=self._format_product_mart(mart_data.get("product_mart", [])),
            order_mart_summary=self._format_order_mart(
                mart_data.get("order_mart_summary", {}),
                mart_data.get("payment_distribution", []),
            ),
        )

    def generate_report(self, mart_data: Dict[str, Any], execution_date: str) -> str:
        """마트 데이터 기반 LLM 리포트 생성

        Args:
            mart_data: 마트 4종 집계 데이터
            execution_date: 대상 날짜

        Returns:
            생성된 자연어 비즈니스 리포트 텍스트
        """
        user_prompt = self.build_prompt(mart_data, execution_date)
        logger.info(
            "[LLMDailyReporter] LLM 리포트 생성 시작 — provider=%s, model=%s, date=%s",
            self.provider, self.llm.model, execution_date,
        )

        try:
            report_text = self.llm.generate(
                system_prompt=SYSTEM_PROMPT,
                user_prompt=user_prompt,
                max_tokens=2000,
            )
            logger.info("[LLMDailyReporter] 리포트 생성 완료 (%d자)", len(report_text))
            return report_text
        except RuntimeError as e:
            logger.error("[LLMDailyReporter] LLM 리포트 생성 실패: %s", str(e))
            # 실패 시 폴백 리포트
            return self.llm._generate_fallback(user_prompt)

    def send_to_slack(self, report_text: str, execution_date: str) -> bool:
        """생성된 리포트를 Slack으로 발송

        Args:
            report_text: LLM이 생성한 리포트 텍스트
            execution_date: 대상 날짜

        Returns:
            발송 성공 여부
        """
        from scripts.slack_alert import SlackAlert

        slack = SlackAlert()

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📈 AI 일간 매출 리포트 ({execution_date})",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": report_text[:3000],  # Slack 블록 텍스트 제한
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"🤖 Generated by {self.llm.model} | "
                            f"ecommerce-event-pipeline | "
                            f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
                        ),
                    }
                ],
            },
        ]

        payload = {"channel": slack.channel, "blocks": blocks}
        return slack._post(payload)

    def save_report(self, report_text: str, execution_date: str, output_dir: str) -> str:
        """리포트를 JSON 파일로 저장

        Args:
            report_text: LLM이 생성한 리포트 텍스트
            execution_date: 대상 날짜
            output_dir: 저장 디렉토리 경로

        Returns:
            저장된 파일 경로
        """
        os.makedirs(output_dir, exist_ok=True)
        report_data = {
            "execution_date": execution_date,
            "provider": self.provider,
            "model": self.llm.model,
            "generated_at": datetime.now().isoformat(),
            "report_text": report_text,
        }
        filepath = os.path.join(output_dir, f"llm_report_{execution_date}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info("[LLMDailyReporter] 리포트 저장: %s", filepath)
        return filepath
