import requests
import sys


def send_telegram_notification(bot_token, chat_id, message):
    """Sends a Telegram notification using a bot."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
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


if __name__ == "__main__":
    # Replace with your Telegram bot token and chat ID
    BOT_TOKEN = "7175111231:AAHogvb8j4Tyuf1gu7V9dgzq1CC9VSybbC4"
    CHAT_ID_1 = "5889045582"
    CHAT_ID_2 = "1060518106"

    # Custom message
    message = "Database Migration Process Completed Successfully! ✅"

    send_telegram_notification(BOT_TOKEN, CHAT_ID_1, message)
    send_telegram_notification(BOT_TOKEN, CHAT_ID_2, message)
    sys.exit(0)
