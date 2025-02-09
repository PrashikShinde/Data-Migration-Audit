import requests

def send_telegram_notification(bot_token, chat_ids, message):
    """Sends a Telegram notification using a bot."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    for chat_id in chat_ids:
        payload = {"chat_id": chat_id, "text": message}
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                print("[INFO] Notification sent successfully!")
            else:
                print(f"[ERROR] Notification failed: {response.text}")
        except Exception as e:
            print(f"[ERROR] Exception occurred while sending notification: {e}")

BOT_TOKEN = "7175111231:AAHogvb8j4Tyuf1gu7V9dgzq1CC9VSybbC4"
CHAT_IDS = ["5889045582", "1060518106"]  # Prashik(5889045582) & Chinmay(1060518106)

if __name__ == "__main__":
    send_telegram_notification(BOT_TOKEN, CHAT_IDS, "🔔 Notification system is active.")
