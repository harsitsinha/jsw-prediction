
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from main import fetch_history, fetch_realtime
from Weather import fetch_realtime_weather
from predictions import fetch_weather_predicition
from flask_cors import CORS
from dotenv import load_dotenv
import os
import asyncio

app = Flask(__name__)
CORS(app)
load_dotenv()

# Create a scheduler
scheduler = BackgroundScheduler()

# Define the fetch_realtime job
@scheduler.scheduled_job('interval', hours=24)
def fetch_realtime_job():
    fetch_realtime()
    fetch_realtime_weather()
    fetch_weather_predicition()


# Immediately run the fetch_realtime_job once
fetch_realtime_job()

# Start the scheduler
scheduler.start()

# Define a route in your Flask app to start the scheduler
@app.route('/start_scheduler', methods=['GET'])
def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        return 'Scheduler started'
    else:
        return 'Scheduler is already running'

# Define a route to stop the scheduler (optional)
@app.route('/stop_scheduler', methods=['GET'])
def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        return 'Scheduler stopped'
    else:
        return 'Scheduler is not running'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)