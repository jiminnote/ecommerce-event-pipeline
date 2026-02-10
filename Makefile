.PHONY: help setup down test test-cov generate generate-month validate spark-batch dashboard clean

help: ## 도움말
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Docker Compose 환경 구축 (Airflow + PostgreSQL)
	docker-compose up -d
	@echo ""
	@echo "Airflow UI: http://localhost:8080 (admin / admin)"

down: ## Docker 환경 종료 및 볼륨 삭제
	docker-compose down -v

test: ## 전체 테스트 실행
	python -m pytest tests/ -v --tb=short

test-cov: ## 커버리지 포함 테스트
	python -m pytest tests/ -v --cov=scripts --cov-report=term-missing

generate: ## 오늘 날짜 이벤트 생성
	python scripts/generate_events.py --date $$(date +%Y-%m-%d) --users 500

generate-month: ## 2026-01 한 달치 이벤트 일괄 생성
	python scripts/generate_events.py --start-date 2026-01-01 --end-date 2026-01-31 --users 500

validate: ## 가장 최근 이벤트 파일 품질 검증
	@FILE=$$(ls -t data/events/*.jsonl 2>/dev/null | head -1); \
	if [ -z "$$FILE" ]; then echo "이벤트 파일이 없습니다. 먼저 make generate 를 실행하세요."; exit 1; fi; \
	echo "검증 대상: $$FILE"; \
	python scripts/validate_quality.py "$$FILE"

spark-batch: ## Spark 배치 집계 (data/events → data/spark_output)
	spark-submit scripts/spark_batch_processor.py \
		--input data/events/ \
		--output data/spark_output \
		--date $$(date +%Y-%m-%d)

dashboard: ## 품질 대시보드 HTML 생성
	python scripts/quality_dashboard.py --source local --input data/reports --html data/reports/dashboard.html

clean: ## 생성된 데이터 및 리포트 삭제
	rm -rf data/events/*.jsonl data/reports/*.json data/reports/*.html data/spark_output/
	@echo "데이터 정리 완료"
