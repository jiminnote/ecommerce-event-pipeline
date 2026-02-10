"""
PySpark 대용량 이벤트 배치 프로세서
====================================
일별 JSONL 이벤트 로그를 Spark DataFrame으로 로딩하여
대용량 집계 처리를 수행합니다.

소규모(일 수천건)에서는 Python + SQL로 충분하지만,
이벤트 규모가 일 수백만건 이상으로 증가할 때
Spark 기반 병렬 처리로 전환하기 위한 배치 잡입니다.

집계 항목:
  1. 일별 퍼널 전환율 (플랫폼별)
  2. 시간대별 트래픽 분포
  3. 상품별 전환율 TOP N
  4. 사용자 세션 행동 시퀀스 분석

사용법:
  spark-submit scripts/spark_batch_processor.py \
    --input data/events/ \
    --output data/spark_output/ \
    --date 2026-01-15

  # 범위 처리
  spark-submit scripts/spark_batch_processor.py \
    --input data/events/ \
    --output data/spark_output/ \
    --start-date 2026-01-01 --end-date 2026-01-31
"""

import argparse
import os
import sys
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)


# ========== 스키마 정의 ==========

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("platform", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("element_id", StringType(), True),
    StructField("element_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_id", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("search_query", StringType(), True),
    StructField("result_count", IntegerType(), True),
    StructField("referrer", StringType(), True),
    StructField("extra_data", StringType(), True),
])


class SparkBatchProcessor:
    """PySpark 기반 대용량 이벤트 집계 프로세서"""

    def __init__(self, app_name: str = "EcommerceEventProcessor"):
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "Asia/Seoul")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

    def load_events(self, input_path: str, dates: list[str] = None):
        """JSONL 이벤트 파일 로딩

        Args:
            input_path: 이벤트 파일 디렉토리 경로
            dates: 로딩할 날짜 목록 (None이면 전체)

        Returns:
            타임스탬프 파싱 및 event_date 컬럼이 추가된 DataFrame
        """
        if dates:
            paths = [
                os.path.join(input_path, f"events_{d.replace('-', '')}.jsonl")
                for d in dates
            ]
            paths = [p for p in paths if os.path.exists(p)]
            if not paths:
                raise FileNotFoundError(f"지정 날짜에 해당하는 파일 없음: {dates}")
            df = self.spark.read.schema(EVENT_SCHEMA).json(paths)
        else:
            df = self.spark.read.schema(EVENT_SCHEMA).json(
                os.path.join(input_path, "*.jsonl")
            )

        df = (
            df.withColumn("ts", F.to_timestamp("timestamp"))
              .withColumn("event_date", F.to_date("ts"))
              .withColumn("event_hour", F.hour("ts"))
        )

        record_count = df.count()
        date_range = df.select(
            F.min("event_date").alias("min_date"),
            F.max("event_date").alias("max_date"),
        ).first()

        print(f"[로딩 완료] {record_count:,}건 "
              f"({date_range['min_date']} ~ {date_range['max_date']})")

        return df

    def compute_funnel_conversion(self, df):
        """일별 / 플랫폼별 전환 퍼널 집계

        Returns:
            DataFrame: funnel_date, platform, step1~4 유저수, 단계별 전환율
        """
        funnel = (
            df.groupBy("event_date", "platform")
            .agg(
                F.countDistinct(
                    F.when(F.col("event_type") == "page_view", F.col("user_id"))
                ).alias("step1_viewers"),
                F.countDistinct(
                    F.when(F.col("event_type") == "click", F.col("user_id"))
                ).alias("step2_clickers"),
                F.countDistinct(
                    F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))
                ).alias("step3_cart_adders"),
                F.countDistinct(
                    F.when(F.col("event_type") == "purchase", F.col("user_id"))
                ).alias("step4_purchasers"),
            )
            .withColumn(
                "view_to_click_rate",
                F.round(F.col("step2_clickers") / F.col("step1_viewers") * 100, 2),
            )
            .withColumn(
                "click_to_cart_rate",
                F.round(F.col("step3_cart_adders") / F.col("step2_clickers") * 100, 2),
            )
            .withColumn(
                "cart_to_purchase_rate",
                F.round(F.col("step4_purchasers") / F.col("step3_cart_adders") * 100, 2),
            )
            .withColumn(
                "overall_conversion_rate",
                F.round(F.col("step4_purchasers") / F.col("step1_viewers") * 100, 2),
            )
            .orderBy("event_date", "platform")
        )

        print(f"[퍼널 집계] {funnel.count()}행")
        return funnel

    def compute_hourly_traffic(self, df):
        """시간대별 트래픽 분포 집계

        Returns:
            DataFrame: event_date, event_hour, platform, event_count,
                       unique_users, unique_sessions
        """
        hourly = (
            df.groupBy("event_date", "event_hour", "platform")
            .agg(
                F.count("event_id").alias("event_count"),
                F.countDistinct("user_id").alias("unique_users"),
                F.countDistinct("session_id").alias("unique_sessions"),
            )
            .orderBy("event_date", "event_hour", "platform")
        )

        print(f"[시간대 트래픽] {hourly.count()}행")
        return hourly

    def compute_product_conversion(self, df, top_n: int = 20):
        """상품별 전환율 TOP N 집계

        click 수 대비 purchase 전환율이 높은 상품을 추출합니다.
        purchase 이벤트의 상품은 extra_data JSON에서 파싱합니다.

        Returns:
            DataFrame: product_id, clicks, carts, purchases,
                       revenue, conversion_rate
        """
        # click/add_to_cart 이벤트에서 직접 참조
        direct_events = (
            df.filter(
                (F.col("product_id").isNotNull()) &
                (F.col("event_type").isin("click", "add_to_cart"))
            )
            .select("user_id", "product_id", "event_type", "quantity", "unit_price")
        )

        # purchase 이벤트: extra_data에서 상품 배열 추출
        purchase_items = (
            df.filter(F.col("event_type") == "purchase")
            .filter(F.col("extra_data").isNotNull())
            .select(
                "user_id",
                F.from_json(
                    F.col("extra_data"),
                    "products ARRAY<STRUCT<product_id: STRING, quantity: INT, unit_price: DOUBLE>>, "
                    "discount_amount: DOUBLE, shipping_fee: DOUBLE, coupon_code: STRING",
                ).alias("parsed"),
            )
            .select("user_id", F.explode("parsed.products").alias("item"))
            .select(
                "user_id",
                F.col("item.product_id").alias("product_id"),
                F.lit("purchase").alias("event_type"),
                F.col("item.quantity").alias("quantity"),
                F.col("item.unit_price").alias("unit_price"),
            )
        )

        all_product_events = direct_events.unionByName(purchase_items)

        product_stats = (
            all_product_events.groupBy("product_id")
            .agg(
                F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
                F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carts"),
                F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                F.sum(
                    F.when(
                        F.col("event_type") == "purchase",
                        F.col("quantity") * F.col("unit_price"),
                    ).otherwise(0)
                ).alias("revenue"),
                F.countDistinct(
                    F.when(F.col("event_type") == "click", F.col("user_id"))
                ).alias("unique_viewers"),
                F.countDistinct(
                    F.when(F.col("event_type") == "purchase", F.col("user_id"))
                ).alias("unique_buyers"),
            )
            .withColumn(
                "conversion_rate",
                F.round(F.col("unique_buyers") / F.col("unique_viewers") * 100, 2),
            )
            .orderBy(F.desc("revenue"))
            .limit(top_n)
        )

        print(f"[상품 전환율] TOP {top_n}")
        return product_stats

    def compute_session_sequences(self, df):
        """사용자 세션별 이벤트 시퀀스 분석

        세션별로 이벤트 순서를 수집하여 행동 패턴을 분석합니다.
        예: page_view -> click -> add_to_cart -> purchase

        Returns:
            DataFrame: session_id, user_id, platform, event_sequence,
                       event_count, session_duration_seconds, has_purchase
        """
        w = Window.partitionBy("session_id").orderBy("ts")

        session_events = (
            df.withColumn("event_order", F.row_number().over(w))
            .groupBy("session_id", "user_id", "platform")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(F.col("event_order"), F.col("event_type"))
                    )
                ).alias("ordered_events"),
                F.count("event_id").alias("event_count"),
                F.min("ts").alias("session_start"),
                F.max("ts").alias("session_end"),
                F.max(
                    F.when(F.col("event_type") == "purchase", F.lit(True))
                    .otherwise(F.lit(False))
                ).alias("has_purchase"),
            )
            .withColumn(
                "event_sequence",
                F.concat_ws(
                    " -> ",
                    F.transform("ordered_events", lambda x: x["event_type"]),
                ),
            )
            .withColumn(
                "session_duration_seconds",
                F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
            )
            .select(
                "session_id", "user_id", "platform",
                "event_sequence", "event_count",
                "session_duration_seconds", "has_purchase",
            )
        )

        # 세션 패턴별 빈도
        pattern_summary = (
            session_events.groupBy("event_sequence")
            .agg(
                F.count("session_id").alias("session_count"),
                F.round(F.avg("session_duration_seconds"), 1).alias("avg_duration_sec"),
                F.round(F.avg(F.col("has_purchase").cast("int")) * 100, 2).alias("purchase_rate"),
            )
            .orderBy(F.desc("session_count"))
        )

        print(f"[세션 시퀀스] 패턴 {pattern_summary.count()}종")
        return session_events, pattern_summary

    def save_results(self, df, output_path: str, name: str):
        """집계 결과를 Parquet + CSV로 저장"""
        parquet_path = os.path.join(output_path, name, "parquet")
        csv_path = os.path.join(output_path, name, "csv")

        df.write.mode("overwrite").parquet(parquet_path)

        # 리뷰/디버그용 CSV (소규모)
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(csv_path)
        )

        print(f"  -> {name}: {parquet_path}")

    def run(self, input_path: str, output_path: str, dates: list[str] = None):
        """전체 배치 처리 실행"""
        print("=" * 60)
        print(f"Spark 배치 프로세서 시작")
        print(f"  입력: {input_path}")
        print(f"  출력: {output_path}")
        print("=" * 60)

        # 1. 데이터 로딩
        df = self.load_events(input_path, dates)
        df.cache()

        # 2. 퍼널 전환율
        funnel = self.compute_funnel_conversion(df)
        self.save_results(funnel, output_path, "funnel_conversion")

        # 3. 시간대별 트래픽
        hourly = self.compute_hourly_traffic(df)
        self.save_results(hourly, output_path, "hourly_traffic")

        # 4. 상품별 전환율
        product = self.compute_product_conversion(df, top_n=20)
        self.save_results(product, output_path, "product_conversion")

        # 5. 세션 시퀀스 분석
        sessions, patterns = self.compute_session_sequences(df)
        self.save_results(patterns, output_path, "session_patterns")

        # 요약 출력
        summary = {
            "total_events": df.count(),
            "unique_users": df.select("user_id").distinct().count(),
            "unique_sessions": df.select("session_id").distinct().count(),
            "funnel_rows": funnel.count(),
            "hourly_rows": hourly.count(),
            "session_patterns": patterns.count(),
        }

        print("\n" + "=" * 60)
        print("배치 처리 완료")
        for k, v in summary.items():
            print(f"  {k}: {v:,}")
        print("=" * 60)

        df.unpersist()
        return summary

    def stop(self):
        self.spark.stop()


def _build_date_list(start: str, end: str) -> list[str]:
    """날짜 범위를 리스트로 변환"""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    dates = []
    current = s
    while current <= e:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def main():
    parser = argparse.ArgumentParser(description="PySpark 이벤트 배치 프로세서")
    parser.add_argument("--input", type=str, default="data/events",
                        help="이벤트 JSONL 디렉토리")
    parser.add_argument("--output", type=str, default="data/spark_output",
                        help="집계 결과 출력 디렉토리")
    parser.add_argument("--date", type=str, help="처리 날짜 (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="시작 날짜")
    parser.add_argument("--end-date", type=str, help="종료 날짜")
    args = parser.parse_args()

    dates = None
    if args.date:
        dates = [args.date]
    elif args.start_date and args.end_date:
        dates = _build_date_list(args.start_date, args.end_date)

    processor = SparkBatchProcessor()
    try:
        processor.run(args.input, args.output, dates)
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
