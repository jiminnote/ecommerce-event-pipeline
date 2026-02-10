"""
품질 대시보드 리포트 생성기
====================================
quality_check_log 테이블 또는 로컬 리포트 JSON에서
품질 트렌드를 집계하여 터미널 리포트 + HTML 대시보드를 생성합니다.

Metabase/Tableau 없이도 파이프라인 품질 현황을
한눈에 파악할 수 있는 경량 대시보드입니다.

사용법:
  # 로컬 리포트 JSON 기반
  python scripts/quality_dashboard.py --source local --input data/reports/

  # PostgreSQL quality_check_log 기반 (Docker 환경)
  python scripts/quality_dashboard.py --source db --days 30
"""

import json
import os
import sys
import glob
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List


class QualityDashboard:
    """품질 검증 결과 대시보드"""

    def __init__(self):
        self.daily_reports: Dict[str, Dict] = {}  # date -> report

    def load_from_local(self, report_dir: str):
        """로컬 JSON 리포트 파일에서 로딩"""
        pattern = os.path.join(report_dir, "pipeline_report_*.json")
        files = sorted(glob.glob(pattern))

        if not files:
            print(f"[WARN] 리포트 파일 없음: {pattern}")
            return

        for filepath in files:
            with open(filepath, "r", encoding="utf-8") as f:
                report = json.load(f)
            date = report.get("report_date") or os.path.basename(filepath).split("_")[-1].replace(".json", "")
            self.daily_reports[date] = report

        print(f"[로딩] {len(self.daily_reports)}일치 리포트 로딩 완료")

    def load_from_db(self, conn_string: str = None, days: int = 30):
        """PostgreSQL quality_check_log에서 로딩

        Docker 환경에서 실행 시:
          conn_string = "host=localhost port=5432 dbname=ecommerce user=airflow password=airflow"
        """
        try:
            import psycopg2
        except ImportError:
            print("[ERROR] psycopg2 미설치. pip install psycopg2-binary")
            return

        conn_str = conn_string or (
            "host=localhost port=5432 dbname=ecommerce user=airflow password=airflow"
        )

        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT check_date, check_name, check_type, target_table,
                   total_records, failed_records, pass_rate, status, detail
            FROM quality_check_log
            WHERE check_date >= CURRENT_DATE - %s
            ORDER BY check_date, check_id
        """, (days,))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            date = row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])
            if date not in self.daily_reports:
                self.daily_reports[date] = {"quality": {"checks": []}, "report_date": date}
            self.daily_reports[date]["quality"]["checks"].append({
                "check_name": row[1],
                "check_type": row[2],
                "target_table": row[3],
                "total_records": row[4],
                "failed_records": row[5],
                "pass_rate": float(row[6]) if row[6] else 0,
                "status": row[7],
                "detail": row[8] or "",
            })

        # overall_status 재계산
        for date, report in self.daily_reports.items():
            checks = report.get("quality", {}).get("checks", [])
            if checks:
                all_pass = all(c["status"] == "PASS" for c in checks)
                report["quality"]["overall_status"] = "PASS" if all_pass else "FAIL"
                report["quality"]["total_checks"] = len(checks)
                report["quality"]["passed"] = sum(1 for c in checks if c["status"] == "PASS")

        print(f"[DB 로딩] {len(self.daily_reports)}일치 품질 로그")

    def generate_terminal_report(self) -> str:
        """터미널 출력용 텍스트 리포트"""
        if not self.daily_reports:
            return "[WARN] 리포트 데이터 없음"

        lines = []
        lines.append("=" * 70)
        lines.append("품질 대시보드 리포트")
        lines.append(f"기간: {min(self.daily_reports.keys())} ~ {max(self.daily_reports.keys())}")
        lines.append(f"총 {len(self.daily_reports)}일")
        lines.append("=" * 70)

        # 일별 요약
        lines.append("")
        lines.append("--- 일별 품질 현황 ---")
        lines.append(f"{'날짜':12s} {'상태':6s} {'통과':8s} {'이벤트':>10s} {'구매':>6s} {'매출':>14s}")
        lines.append("-" * 70)

        pass_days = 0
        fail_days = 0
        total_events = 0
        total_revenue = 0

        for date in sorted(self.daily_reports.keys()):
            report = self.daily_reports[date]
            quality = report.get("quality", {})
            events = report.get("events", {})

            status = quality.get("overall_status", "N/A")
            passed = quality.get("passed", 0)
            total_checks = quality.get("total_checks", 0)
            evt_count = events.get("total_events", 0)
            purchases = events.get("purchase_count", 0)
            revenue = events.get("total_revenue", 0)

            if status == "PASS":
                pass_days += 1
            elif status == "FAIL":
                fail_days += 1
            total_events += evt_count
            total_revenue += revenue

            lines.append(
                f"{date:12s} {status:6s} {passed}/{total_checks:5d}"
                f" {evt_count:>10,d} {purchases:>6,d} {revenue:>14,.0f}"
            )

        lines.append("-" * 70)

        # 통계 요약
        lines.append("")
        lines.append("--- 기간 통계 ---")
        lines.append(f"  PASS 일수:       {pass_days}일")
        lines.append(f"  FAIL 일수:       {fail_days}일")
        lines.append(f"  가용률:          {pass_days / max(pass_days + fail_days, 1) * 100:.1f}%")
        lines.append(f"  총 이벤트:       {total_events:,}건")
        lines.append(f"  총 매출:         {total_revenue:,.0f}원")

        # 검증 항목별 실패 빈도
        lines.append("")
        lines.append("--- 검증 항목별 실패 빈도 ---")
        check_failures = defaultdict(int)
        check_total = defaultdict(int)
        for report in self.daily_reports.values():
            for check in report.get("quality", {}).get("checks", []):
                check_total[check["check_name"]] += 1
                if check["status"] == "FAIL":
                    check_failures[check["check_name"]] += 1

        for name in sorted(check_total.keys()):
            fails = check_failures.get(name, 0)
            total = check_total[name]
            rate = (total - fails) / total * 100 if total else 0
            marker = " <-- 주의" if fails > 0 else ""
            lines.append(f"  {name:30s} {rate:6.1f}% ({fails}회 실패 / {total}회){marker}")

        lines.append("")
        lines.append("=" * 70)

        return "\n".join(lines)

    def generate_html_dashboard(self, output_path: str):
        """HTML 대시보드 파일 생성"""
        dates = sorted(self.daily_reports.keys())

        # 일별 데이터 준비
        statuses = []
        event_counts = []
        pass_rates = []

        for date in dates:
            report = self.daily_reports[date]
            quality = report.get("quality", {})
            events = report.get("events", {})

            statuses.append(1 if quality.get("overall_status") == "PASS" else 0)
            event_counts.append(events.get("total_events", 0))

            checks = quality.get("checks", [])
            if checks:
                avg_rate = sum(c.get("pass_rate", 0) for c in checks) / len(checks)
            else:
                avg_rate = 0
            pass_rates.append(round(avg_rate, 2))

        html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>품질 대시보드</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 900px; margin: 40px auto; padding: 0 20px;
         background: #f8f9fa; color: #333; }}
  h1 {{ border-bottom: 2px solid #333; padding-bottom: 10px; }}
  .summary {{ display: flex; gap: 16px; margin: 20px 0; }}
  .card {{ background: #fff; border-radius: 8px; padding: 16px 24px;
           box-shadow: 0 1px 3px rgba(0,0,0,0.1); flex: 1; }}
  .card .label {{ font-size: 12px; color: #888; text-transform: uppercase; }}
  .card .value {{ font-size: 28px; font-weight: bold; margin-top: 4px; }}
  .pass {{ color: #28a745; }}
  .fail {{ color: #dc3545; }}
  table {{ width: 100%; border-collapse: collapse; margin: 20px 0;
           background: #fff; border-radius: 8px; overflow: hidden;
           box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  th {{ background: #343a40; color: #fff; padding: 10px 12px; text-align: left;
       font-size: 13px; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #eee; font-size: 13px; }}
  tr:hover td {{ background: #f1f3f5; }}
  .status-pass {{ color: #28a745; font-weight: bold; }}
  .status-fail {{ color: #dc3545; font-weight: bold; }}
  .section {{ margin: 30px 0; }}
  .bar {{ display: inline-block; height: 16px; background: #28a745;
          border-radius: 2px; }}
  .bar-bg {{ display: inline-block; height: 16px; background: #eee;
             border-radius: 2px; width: 100px; }}
</style>
</head>
<body>
<h1>품질 대시보드</h1>
<p>{min(dates) if dates else 'N/A'} ~ {max(dates) if dates else 'N/A'} ({len(dates)}일)</p>

<div class="summary">
  <div class="card">
    <div class="label">PASS 일수</div>
    <div class="value pass">{sum(statuses)}일</div>
  </div>
  <div class="card">
    <div class="label">FAIL 일수</div>
    <div class="value fail">{len(statuses) - sum(statuses)}일</div>
  </div>
  <div class="card">
    <div class="label">가용률</div>
    <div class="value">{sum(statuses) / max(len(statuses), 1) * 100:.1f}%</div>
  </div>
  <div class="card">
    <div class="label">총 이벤트</div>
    <div class="value">{sum(event_counts):,}</div>
  </div>
</div>

<div class="section">
<h2>일별 현황</h2>
<table>
<tr><th>날짜</th><th>상태</th><th>평균 통과율</th><th>이벤트 수</th><th>통과율 바</th></tr>
"""

        for i, date in enumerate(dates):
            status = "PASS" if statuses[i] else "FAIL"
            css = "status-pass" if statuses[i] else "status-fail"
            bar_width = int(pass_rates[i])
            html += f"""<tr>
<td>{date}</td>
<td class="{css}">{status}</td>
<td>{pass_rates[i]:.1f}%</td>
<td>{event_counts[i]:,}</td>
<td><div class="bar-bg"><div class="bar" style="width:{bar_width}px"></div></div></td>
</tr>\n"""

        # 검증 항목별 실패 빈도
        check_failures = defaultdict(int)
        check_total = defaultdict(int)
        for report in self.daily_reports.values():
            for check in report.get("quality", {}).get("checks", []):
                check_total[check["check_name"]] += 1
                if check["status"] == "FAIL":
                    check_failures[check["check_name"]] += 1

        html += """</table>
</div>

<div class="section">
<h2>검증 항목별 통과율</h2>
<table>
<tr><th>검증 항목</th><th>실행 횟수</th><th>실패 횟수</th><th>통과율</th></tr>
"""

        for name in sorted(check_total.keys()):
            fails = check_failures.get(name, 0)
            total = check_total[name]
            rate = (total - fails) / total * 100 if total else 0
            css = "status-pass" if fails == 0 else "status-fail"
            html += f'<tr><td>{name}</td><td>{total}</td>'
            html += f'<td class="{css}">{fails}</td><td>{rate:.1f}%</td></tr>\n'

        html += """</table>
</div>

<footer style="margin-top:40px;padding:20px 0;border-top:1px solid #ddd;color:#888;font-size:12px;">
  Generated by ecommerce-event-pipeline/quality_dashboard.py
</footer>
</body>
</html>"""

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"[HTML 대시보드] {output_path}")


def main():
    parser = argparse.ArgumentParser(description="품질 대시보드 리포트 생성")
    parser.add_argument("--source", choices=["local", "db"], default="local",
                        help="데이터 소스 (local: JSON 파일, db: PostgreSQL)")
    parser.add_argument("--input", type=str, default="data/reports",
                        help="리포트 JSON 디렉토리 (source=local)")
    parser.add_argument("--conn", type=str, default=None,
                        help="PostgreSQL 접속 문자열 (source=db)")
    parser.add_argument("--days", type=int, default=30,
                        help="조회 기간 일수 (source=db)")
    parser.add_argument("--html", type=str, default="data/reports/dashboard.html",
                        help="HTML 대시보드 출력 경로")
    args = parser.parse_args()

    dashboard = QualityDashboard()

    if args.source == "local":
        dashboard.load_from_local(args.input)
    else:
        dashboard.load_from_db(args.conn, args.days)

    # 터미널 리포트 출력
    terminal_report = dashboard.generate_terminal_report()
    print(terminal_report)

    # HTML 대시보드 생성
    if dashboard.daily_reports:
        dashboard.generate_html_dashboard(args.html)


if __name__ == "__main__":
    main()
