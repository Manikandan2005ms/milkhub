import subprocess
import time
import sys
import os

def start_services():
    print("--- Starting AI Smart Dairy Platform Services ---")
    
    # 1. Start Flask App
    print("Starting Flask Web Server (app.py)...")
    app_env = os.environ.copy()
    app_env["FLASK_ENV"] = "development"
    app_process = subprocess.Popen([sys.executable, "app.py"], env=app_env)
    
    # Give the app a moment to start
    time.sleep(2)
    
    # 2. Start Telegram Bot
    print("Starting Milk Angel Bot (bot.py)...")
    bot_process = subprocess.Popen([sys.executable, "bot.py"])
    
    # 3. Start Scheduler
    print("Starting Automated Summary Scheduler (scheduler.py)...")
    sched_process = subprocess.Popen([sys.executable, "scheduler.py"])

    print("\n--- All services are running! ---")
    print("- Website: http://127.0.0.1:5000")
    print("- Telegram Bot: Activated")
    print("- Scheduler: Active (Daily @ 08:00 PM, Monthly @ 1st 09:00 AM)")
    print("\nPress Ctrl+C to stop services.")
    
    try:
        # Keep the script alive so the background processes stay alive
        while True:
            if app_process.poll() is not None:
                print("WARNING: Flask process stopped unexpectedly.")
                break
            if bot_process.poll() is not None:
                print("WARNING: Bot process stopped unexpectedly.")
                break
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping services...")
    finally:
        app_process.terminate()
        bot_process.terminate()
        print("All processes stopped.")

if __name__ == "__main__":
    start_services()
