from torpy.http.requests import TorRequests


def send_telegram_notification(bot_token, chat_ids, message):
    """
    Sends a Telegram notification using a bot via the Tor network.

    Args:
        bot_token (str): Your Telegram bot token.
        chat_ids (list): List of chat IDs to send the message to.
        message (str): The notification message.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    # Use TorRequests as a context manager for automatic setup and teardown.
    with TorRequests() as tor_requests:
        # Get a session that routes HTTP requests through Tor.
        with tor_requests.get_session() as session:
            for chat_id in chat_ids:
                payload = {"chat_id": chat_id, "text": message}
                try:
                    response = session.post(url, json=payload, timeout=10)
                    if response.status_code == 200:
                        print(f"[INFO] Notification sent successfully to chat_id {chat_id}!")
                    else:
                        print(f"[ERROR] Notification failed for chat_id {chat_id}: {response.text}")
                except Exception as e:
                    print(f"[ERROR] Exception for chat_id {chat_id}: {e}")


if __name__ == "__main__":
    # Replace these with your actual Telegram bot token and chat IDs.
    BOT_TOKEN = "7175111231:AAHogvb8j4Tyuf1gu7V9dgzg1CC9VSybbC4"
    CHAT_IDS = ["5889045582", "1060518106Database_Status.xlsx"]

    send_telegram_notification(BOT_TOKEN, CHAT_IDS, "🔔 Notification system is active using Tor!")
