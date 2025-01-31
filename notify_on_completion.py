import requests
import sys
import time
import datetime
import subprocess
import os
from flask import Flask, request
import threading

app = Flask(__name__)

progress = 0  # Global variable to track execution progress

BOT_TOKEN = "7175111231:AAHogvb8j4Tyuf1gu7V9dgzq1CC9VSybbC4"
CHAT_IDS = ["5889045582", "1060518106"]  # Add multiple chat IDs here


def send_telegram_notification(bot_token, chat_ids, message):
    """Sends a Telegram notification using a bot."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    for chat_id in chat_ids:
        payload = {
            "chat_id": chat_id,
            "text": message
        }
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                print("[INFO] Notification sent successfully!")
            else:
                print(f"[ERROR] Notification failed: {response.text}")
        except Exception as e:
            print(f"[ERROR] Exception occurred while sending notification: {e}")


def process_task():
    global progress
    total_steps = 7
    step = 0

    try:
        print("[INFO] Establishing database connections...")
        # Simulating connection setup
        time.sleep(2)
        print("[INFO] Connection established successfully!")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = f"audit_results_{timestamp}"
        os.makedirs(results_dir, exist_ok=True)

        steps = [
            "Validating Schema...",
            "Checking Row Counts...",
            "Performing Aggregate Checks...",
            "Running SQL Join Validations...",
            "Comparing Data...",
            "Checking for NULL Values..."
        ]

        for step, message in enumerate(steps, 1):
            progress = int((step / total_steps) * 100)
            send_telegram_notification(BOT_TOKEN, CHAT_IDS, f"📊 Progress: {progress}% - {message}")
            time.sleep(2)  # Simulating processing time

        send_telegram_notification(BOT_TOKEN, CHAT_IDS, "✅ Data Migration Audit Completed Successfully!")

    except Exception as e:
        send_telegram_notification(BOT_TOKEN, CHAT_IDS, f"⚠️ Error: {str(e)}")
        print(f"[ERROR] Migration failed: {e}")


@app.route(f"/bot{BOT_TOKEN}", methods=["POST"])
def receive_telegram_update():
    global progress
    data = request.json
    if "message" in data and "text" in data["message"]:
        chat_id = data["message"]["chat"]["id"]
        text = data["message"]["text"].strip()

        if text == "/status":
            send_telegram_notification(BOT_TOKEN, [chat_id], f"Execution Progress: {progress}%")
    return "OK", 200


if __name__ == "__main__":
    # Start the execution process in a separate thread
    threading.Thread(target=process_task, daemon=True).start()

    # Start Flask server to listen for Telegram bot commands
    app.run(host="0.0.0.0", port=5000)
