from apscheduler.schedulers.background import BackgroundScheduler
import time
import os
import mysql.connector
import requests
from datetime import date
from sample_data_generator import generate_session_data

# Config
TELEGRAM_BOT_TOKEN = "Bot Token"
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Mmani2005@")
DB_NAME = os.getenv("DB_NAME", "milk_collection")

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME
    )

def send_telegram(chat_id, text):
    if not chat_id: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Failed to send reminder to {chat_id}: {e}")

def check_and_remind(session_type="FN"):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    sess_text = "morning" if session_type == "FN" else "evening"
    time_text = "before session closes" if session_type == "FN" else "before collection closes"

    # Find farmers with Telegram who missed this session
    query = """
        SELECT id, name, telegram_chat_id FROM users 
        WHERE role = 'farmer' AND telegram_chat_id IS NOT NULL
        AND id NOT IN (
            SELECT farmer_id FROM milk_records WHERE date = CURDATE() AND session = %s
        )
    """
    cursor.execute(query, (session_type,))
    missed_farmers = cursor.fetchall()
    
    for f in missed_farmers:
        msg = f"Hello {f['name']},\nYou have not supplied milk this {sess_text}.\nPlease bring milk {time_text}."
        send_telegram(f['telegram_chat_id'], msg)
        print(f"Sent {sess_text} reminder to {f['name']}")
        
    cursor.close()
    conn.close()

def daily_absent_summary():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # 1. Warn farmers who missed both
    query_missed_all = """
        SELECT id, name, telegram_chat_id FROM users 
        WHERE role = 'farmer' AND telegram_chat_id IS NOT NULL
        AND id NOT IN (
            SELECT farmer_id FROM milk_records WHERE date = CURDATE()
        )
    """
    cursor.execute(query_missed_all)
    missed_all = cursor.fetchall()
    
    for f in missed_all:
        msg = "📍 *Important Reminder*:\nNo milk supplied today.\nPlease maintain regular supply to ensure your quality grades and bonuses."
        send_telegram(f['telegram_chat_id'], msg)
    
    # 2. Alert Admin
    cursor.execute("SELECT name FROM users WHERE role = 'admin' AND telegram_chat_id IS NOT NULL")
    admins = cursor.fetchall()
    
    if admins and missed_all:
        absent_names = "\n".join([f"• {f['name']}" for f in missed_all])
        admin_msg = f"📢 *Absentee Report - {date.today()}*\n\nFarmers absent today:\n{absent_names}"
        for a in admins:
            send_telegram(a['telegram_chat_id'], admin_msg)
            
    cursor.close()
    conn.close()

def start_scheduler(blocking=True):
    scheduler = BackgroundScheduler()
    
    # Data Generation (Before collection starts)
    scheduler.add_job(generate_session_data, 'cron', hour=6, minute=0, args=["FN"])
    scheduler.add_job(generate_session_data, 'cron', hour=16, minute=0, args=["AN"])
    
    # Reminders (During collection)
    scheduler.add_job(check_and_remind, 'cron', hour=9, minute=0, args=["FN"])
    scheduler.add_job(check_and_remind, 'cron', hour=18, minute=0, args=["AN"])
    
    # Final Summary
    scheduler.add_job(daily_absent_summary, 'cron', hour=21, minute=0)
    
    scheduler.start()
    print("MilkHub Scheduler Started successfully.")
    
    if blocking:
        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

if __name__ == "__main__":
    start_scheduler()
