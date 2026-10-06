# One image for the API and the workers; the command decides which it runs.
#   docker build -f deploy/Dockerfile -t rogue-api .
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt requirements-kafka.txt requirements-ansible.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-kafka.txt -r requirements-ansible.txt

COPY main.py config.py ./
COPY app ./app
COPY ansible ./ansible

RUN useradd --create-home rogue
USER rogue

EXPOSE 8000
# API by default. Workers override this with: python -m app.worker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
