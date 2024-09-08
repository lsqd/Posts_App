FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD sleep 25 && python3 setup.py && exec hypercorn app:app --bind 0.0.0.0:8080 --workers 4