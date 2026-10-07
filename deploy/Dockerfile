# One image for the API, the workers, the mock CMS and the command-line tools
# (export, snapshot compare, SMx plan, read-only CMS check); the command
# decides which it runs.
#   docker build -f deploy/Dockerfile -t gpon-legacy-api .
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt requirements-kafka.txt requirements-cms.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-kafka.txt -r requirements-cms.txt

COPY main.py config.py ./
COPY app ./app
COPY deploy/demo ./deploy/demo

RUN useradd --create-home rogue
USER rogue

EXPOSE 8000
# API by default. Workers override this with: python -m app.worker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
