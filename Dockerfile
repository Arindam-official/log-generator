FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY ./app.py .
COPY .regex-rules.txt /app/
CMD ["python", "app.py"]