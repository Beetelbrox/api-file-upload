.PHONY: install-dev
install-dev:
	pip install -r requirements.txt
	pip install -e .

.PHONY: start-api
start-api:
	uvicorn api_file_upload.api:app --reload

.PHONY: start-worker
start-worker:
	arq api_file_upload.worker.WorkerSettings