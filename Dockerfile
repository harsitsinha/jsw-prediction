FROM python:3.11
RUN apt-get update && apt-get install -y wget default-jre
RUN pip install --upgrade pip
RUN pip install flask gunicorn flask_cors python-dotenv pandas tabula-py datetime gspread google-auth APScheduler
RUN apt-get install -y ca-certificates
WORKDIR /app
COPY . /app
ENV PORT 5000
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 app:app