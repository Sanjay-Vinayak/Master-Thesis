FROM python:3.9-slim-buster

WORKDIR /app

COPY . /app


RUN pip install --no-cache-dir Flask psycopg2-binary pymongo pandas numpy Flask-Cors

EXPOSE 5000

CMD ["python", "app_hybrid.py"]