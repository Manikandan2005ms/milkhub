from datetime import date
from functools import wraps
import os
import threading
import time
import io

import mysql.connector
import requests
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for, send_file
from predict import ai_engine
import subprocess
import pandas as pd
import numpy as np
import pdfplumber
import io
import notifications


app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "milk_collection_secret_key")

# Configuration
TELEGRAM_BOT_TOKEN = "8651377308:AAGQlFuV67nGQe3MB23E0V4kiRtToyRMR6g"

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Enter DB Password")
DB_NAME = os.getenv("DB_NAME", "milk_collection")

def send_telegram_notification(chat_id, data):
    """Smart notification handler that supports both raw message strings and record data dictionaries."""
    if not chat_id:
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN", "8651377308:AAGQlFuV67nGQe3MB23E0V4kiRtToyRMR6g")
    if not token:
        return
        
    text = ""
    if isinstance(data, dict):
        date_str = data.get("date")
        session_raw = data.get("session", "FN")
        litres = data.get("litres")
        fat = data.get("fat")
        amount = data.get("amount")
        rate = data.get("rate", amount / litres if litres and amount else 0)
        
        water_percent = data.get("water_percent")
        quality = get_quality_rating(fat, water_percent) if water_percent is not None else get_quality_rating(fat, None)
        
        text = (f"🥛 *Milk Entry Added*\n\n"
                f"🗓️ Date: {date_str}\n"
                f"🕒 Session: {'Morning' if session_raw == 'FN' else 'Evening'}\n"
                f"📏 Volume: {litres} L\n"
                f"🧪 Fat: {fat}%\n"
                f"💹 Final Rate: ₹{rate:.2f}\n"
                f"💰 Total Amount: ₹{amount:.2f}\n"
                f"✨ Quality: *{quality}*")
    else:
        # It's a plain string message
        text = str(data)
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Failed to send Telegram notification: {e}")

def get_server_connection():
    """Connect to MySQL without selecting a database."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def get_db_connection():
    """Connect to the milk_collection database."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )


def get_quality_rating(fat, water_percent):
    """Standalone quality rating logic for use in backend and templates."""
    try:
        f = float(fat) if fat is not None else 0
        w = float(water_percent) if water_percent is not None else 0
        if f >= 4.0 and w <= 5.0: return "Excellent"
        if f >= 3.5 and w <= 10.0: return "Good"
        if f >= 3.0 and w <= 15.0: return "Average"
        return "Poor"
    except (ValueError, TypeError):
        return "Unknown"

@app.context_processor
def utility_processor():
    return dict(get_quality_rating=get_quality_rating)


def get_quality_deduction(fat):
    """Return the rate deduction based on milk fat quality."""
    fat_value = float(fat)
    if fat_value >= 4:
        return 0
    if fat_value >= 3:
        return 2
    return 5

def calculate_pricing(litres, fat, water_percent, base_rate, quality_grade):
    """Calculate final rate and amount using the multi-factor smart formula."""
    final_rate = float(base_rate)
    reasons = []

    # 1. Fat Adjustment
    f = float(fat)
    if f >= 5.0:
        final_rate += 5
    elif f >= 4.5:
        final_rate += 3
    elif f < 3.0:
        # Implicitly handled by Quality deduction if desired, 
        # but let's stick to the user's specific rules.
        pass

    # 2. Water Deduction
    w = float(water_percent or 0)
    if w > 20:
        final_rate -= 10
        reasons.append(f"High water mixing (-₹10)")
    elif w > 10:
        final_rate -= 6
        reasons.append(f"Water mixing detected (-₹6)")
    elif w > 5:
        final_rate -= 3
        reasons.append(f"Minor water mixing (-₹3)")

    # 3. Quality Adjustment
    if quality_grade == "Excellent":
        final_rate += 4
    elif quality_grade == "Good":
        final_rate += 0
    elif quality_grade == "Average":
        final_rate -= 2
        reasons.append(f"Average quality (-₹2)")
    elif quality_grade == "Poor":
        final_rate -= 5
        reasons.append(f"Poor quality (-₹5)")

    # 4. Global Low Fat Check (Custom addition based on typical dairy rules if not explicitly in formula but in thoughts)
    # The user said: "Example Logic: ... if fat >= 4.5 ... if water > 5 ... if quality = Poor"
    # I will stick strictly to the prompt's provided list.

    # Minimum Rate Clamp
    final_rate = max(20.0, final_rate)
    amount = float(litres) * final_rate

    return {
        "base_rate": round(float(base_rate), 2),
        "final_rate": round(final_rate, 2),
        "amount": round(amount, 2),
        "deduction": round(float(base_rate) - final_rate, 2),
        "deduction_reason": " | ".join(reasons) if reasons else "Full Rate Applied"
    }


def add_column_if_missing(cursor, table_name, column_name, column_sql):
    cursor.execute(
        """
        SELECT COUNT(*) AS column_count
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND COLUMN_NAME = %s
        """,
        (DB_NAME, table_name, column_name),
    )
    column_exists = cursor.fetchone()[0] > 0

    if not column_exists:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def get_rate_for_date(cursor, record_date):
    cursor.execute(
        """
        SELECT base_rate
        FROM milk_rates
        WHERE date = %s
        """,
        (record_date,),
    )
    return cursor.fetchone()


def create_database_and_tables():
    """Create database, tables, and starter login users."""
    server_connection = get_server_connection()
    server_cursor = server_connection.cursor()
    server_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    server_cursor.close()
    server_connection.close()

    connection = get_db_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            phone VARCHAR(20) NOT NULL UNIQUE,
            role ENUM('admin', 'farmer') NOT NULL,
            village VARCHAR(255) DEFAULT NULL,
            join_date DATE DEFAULT NULL,
            telegram_chat_id VARCHAR(50) DEFAULT NULL,
            telegram_linked BOOLEAN DEFAULT FALSE,
            language VARCHAR(10) DEFAULT 'en'
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS milk_records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            farmer_id INT NOT NULL,
            date DATE NOT NULL,
            `session` ENUM('FN', 'AN') NOT NULL DEFAULT 'FN',
            litres DECIMAL(10, 2) NOT NULL,
            fat DECIMAL(10, 2) NOT NULL,
            snf DECIMAL(10, 2) DEFAULT NULL,
            water_percent DECIMAL(10, 2) DEFAULT NULL,
            base_rate DECIMAL(10, 2) NOT NULL DEFAULT 0,
            deduction DECIMAL(10, 2) NOT NULL DEFAULT 0,
            final_rate DECIMAL(10, 2) NOT NULL DEFAULT 0,
            amount DECIMAL(10, 2) NOT NULL,
            deduction_reason TEXT DEFAULT NULL,
            FOREIGN KEY (farmer_id) REFERENCES users(id) ON DELETE CASCADE
        )

        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS milk_rates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            base_rate DECIMAL(10, 2) NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            farmer_id INT NOT NULL,
            date DATE NOT NULL,
            predicted_litres DECIMAL(10, 2),
            quality_prediction VARCHAR(50),
            fraud_risk VARCHAR(20),
            performance_score INT,
            recommendation TEXT,
            confidence_score VARCHAR(20) DEFAULT 'Medium',
            trend VARCHAR(20) DEFAULT 'Stable',
            FOREIGN KEY (farmer_id) REFERENCES users(id) ON DELETE CASCADE,
            UNIQUE KEY (farmer_id, date)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS farmer_scores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            farmer_id INT NOT NULL,
            score INT DEFAULT 0,
            trend VARCHAR(20) DEFAULT 'Stable',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (farmer_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    add_column_if_missing(
        cursor,
        "milk_records",
        "session",
        "`session` ENUM('FN', 'AN') NOT NULL DEFAULT 'FN' AFTER date",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "snf",
        "snf DECIMAL(10, 2) DEFAULT NULL AFTER fat",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "water_percent",
        "water_percent DECIMAL(10, 2) DEFAULT NULL AFTER snf",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "base_rate",
        "base_rate DECIMAL(10, 2) NOT NULL DEFAULT 0 AFTER water_percent",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "deduction",
        "deduction DECIMAL(10, 2) NOT NULL DEFAULT 0 AFTER base_rate",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "ph",
        "ph DECIMAL(4, 2) DEFAULT 6.6 AFTER water_percent",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "temperature",
        "temperature DECIMAL(5, 2) DEFAULT 35.0 AFTER ph",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "final_rate",
        "final_rate DECIMAL(10, 2) NOT NULL DEFAULT 0 AFTER deduction",
    )
    add_column_if_missing(
        cursor,
        "users",
        "telegram_chat_id",
        "telegram_chat_id VARCHAR(50) DEFAULT NULL",
    )
    add_column_if_missing(
        cursor,
        "milk_records",
        "deduction_reason",
        "deduction_reason TEXT DEFAULT NULL",
    )

    # ML Predictions Migration
    add_column_if_missing(cursor, "ml_predictions", "predicted_litres", "predicted_litres DECIMAL(10, 2)")
    add_column_if_missing(cursor, "ml_predictions", "quality_prediction", "quality_prediction VARCHAR(50)")
    add_column_if_missing(cursor, "ml_predictions", "fraud_risk", "fraud_risk VARCHAR(20)")
    add_column_if_missing(cursor, "ml_predictions", "performance_score", "performance_score INT")
    add_column_if_missing(cursor, "ml_predictions", "recommendation", "recommendation TEXT")
    add_column_if_missing(cursor, "ml_predictions", "confidence_score", "confidence_score VARCHAR(20) DEFAULT 'Medium'")
    add_column_if_missing(cursor, "ml_predictions", "trend", "trend VARCHAR(20) DEFAULT 'Stable'")

    # Farmer Scores Migration
    add_column_if_missing(cursor, "farmer_scores", "score", "score INT DEFAULT 0")
    add_column_if_missing(cursor, "farmer_scores", "trend", "trend VARCHAR(20) DEFAULT 'Stable'")

    cursor.execute(
        """
        UPDATE milk_records
        SET final_rate = amount / litres,
            base_rate = amount / litres
        WHERE final_rate = 0
            AND litres > 0
        """
    )

    # Role migration: operator -> admin
    try:
        cursor.execute("UPDATE users SET role = 'admin' WHERE role = 'operator'")
        # Update Enum if possible (MySQL 8.0+ or specific syntax)
        cursor.execute("ALTER TABLE users MODIFY COLUMN role ENUM('admin', 'farmer') NOT NULL")
    except:
        pass

    starter_users = [
        ("System Admin", "9999999999", "admin", 1),
    ]

    for name, phone, role, center_id in starter_users:
        cursor.execute("SELECT id FROM users WHERE phone = %s", (phone,))
        existing_user = cursor.fetchone()
        if not existing_user:
            cursor.execute(
                """
                INSERT INTO users (name, phone, role, center_id)
                VALUES (%s, %s, %s, %s)
                """,
                (name, phone, role, center_id),
            )

    try:
        cursor.execute("ALTER TABLE milk_records DROP FOREIGN KEY milk_records_ibfk_1")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE milk_records ADD CONSTRAINT milk_records_ibfk_1 FOREIGN KEY (farmer_id) REFERENCES users(id) ON DELETE CASCADE")
    except:
        pass

    connection.commit()
    cursor.close()
    connection.close()

def redirect_to_role_dashboard(user):
    if user["role"] == "admin":
        return redirect(url_for("admin_dashboard"))
    return redirect(url_for("farmer_dashboard", farmer_id=user["id"]))


def current_user():
    if "user_id" not in session:
        return None

    return {
        "id": session["user_id"],
        "name": session["name"],
        "phone": session["phone"],
        "role": session["role"],
    }


def login_required(allowed_roles=None):
    def decorator(route_function):
        @wraps(route_function)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                flash("Please login first.", "warning")
                return redirect(url_for("index"))

            if allowed_roles and user["role"] not in allowed_roles:
                flash("You do not have permission to open that page.", "danger")
                return redirect_to_role_dashboard(user)

            return route_function(*args, **kwargs)

        return wrapper

    return decorator


    return cursor.fetchone()


def get_farmer_ranking(cursor):
    """Get top 10 farmers based on composite score (volume + quality)."""
    cursor.execute("""
        SELECT u.id, u.name, u.phone, 
               COALESCE(s.score, 0) as score, 
               COALESCE(s.trend, 'Stable') as trend,
               SUM(m.litres) as total_litres
        FROM users u
        LEFT JOIN farmer_scores s ON u.id = s.farmer_id
        LEFT JOIN milk_records m ON u.id = m.farmer_id AND m.date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        WHERE u.role = 'farmer'
        GROUP BY u.id, u.name, u.phone, s.score, s.trend
        ORDER BY score DESC, total_litres DESC
        LIMIT 10
    """)
    return cursor.fetchall()

def get_supply_streak(cursor, farmer_id):
    """Calculate consecutive days of milk supply for a farmer."""
    cursor.execute("""
        SELECT DISTINCT date FROM milk_records 
        WHERE farmer_id = %s 
        ORDER BY date DESC
    """, (farmer_id,))
    dates = [r['date'] for r in cursor.fetchall()]
    
    if not dates: return 0
    
    streak = 0
    from datetime import date, timedelta
    current_check = date.today()
    
    # If not supplied today, check if supplied yesterday
    if dates[0] != current_check:
        current_check = current_check - timedelta(days=1)
        if dates[0] != current_check:
            return 0
            
    for d in dates:
        if d == current_check:
            streak += 1
            current_check -= timedelta(days=1)
        else:
            break
    return streak

def get_missed_supply_alerts(cursor):
    """Find farmers who haven't supplied milk for more than 2 days."""
    cursor.execute("""
        SELECT u.name, u.phone, MAX(m.date) as last_date, DATEDIFF(CURDATE(), MAX(m.date)) as days_missed
        FROM users u
        LEFT JOIN milk_records m ON u.id = m.farmer_id
        WHERE u.role = 'farmer'
        GROUP BY u.id, u.name, u.phone
        HAVING days_missed >= 2 OR last_date IS NULL
        ORDER BY days_missed DESC
        LIMIT 10
    """)
    return cursor.fetchall()

def get_dashboard_totals(cursor, extra_where="", params=None):
    """Calculate totals with optional filters (e.g., for a specific farmer)."""
    sql = f"""
        SELECT 
            COALESCE(SUM(litres), 0) as total_milk_today,
            COALESCE(SUM(amount), 0) as total_amount
        FROM milk_records
        WHERE date = CURDATE() {extra_where}
    """
    cursor.execute(sql, params or ())
    totals = cursor.fetchone()
    
    # Month totals if on farmer dashboard
    month_totals = {"total_milk_this_month": 0}
    if "farmer_id" in extra_where:
        cursor.execute(f"""
            SELECT COALESCE(SUM(litres), 0) as total_milk_this_month
            FROM milk_records
            WHERE MONTH(date) = MONTH(CURDATE()) AND YEAR(date) = YEAR(CURDATE()) {extra_where}
        """, params or ())
        month_totals["total_milk_this_month"] = cursor.fetchone()["total_milk_this_month"]

    return {
        "total_milk_today": totals["total_milk_today"],
        "total_amount": totals["total_amount"],
        "total_milk_this_month": month_totals["total_milk_this_month"]
    }

def get_weather_insight():
    """Return a seasonal insight based on current month."""
    from datetime import datetime
    month = datetime.now().month
    if 3 <= month <= 6:
        return "⚠️ Production may reduce due to summer heat. Ensure cattle hydration."
    if 10 <= month <= 11:
        return "🌧️ Rainy season: Keep sheds dry to prevent infections."
    return "✅ Season is favorable for milk production."


@app.route("/")
def index():
    if current_user():
        return redirect_to_role_dashboard(current_user())
    return render_template("login.html")

@app.route("/sw.js")
def service_worker():
    # Enforces global scope required by browsers for caching functionality
    return app.send_static_file("sw.js")


@app.route("/login", methods=["POST"])
def login():
    phone = request.form.get("phone", "").strip()

    if not phone:
        flash("Please enter your mobile number.", "warning")
        return redirect(url_for("index"))

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE phone = %s", (phone,))
    user = cursor.fetchone()
    cursor.close()
    connection.close()

    if not user:
        flash("User not found. Please contact the milk center.", "danger")
        return redirect(url_for("index"))

    session.clear()
    session["user_id"] = user["id"]
    session["name"] = user["name"]
    session["phone"] = user["phone"]
    session["role"] = user["role"]

    flash(f"Welcome, {user['name']}!", "success")
    return redirect_to_role_dashboard(user)


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("index"))


@app.route("/set_rate", methods=["POST"])
@login_required(["admin"])
def set_rate():
    base_rate = request.form.get("base_rate", "").strip()
    rate_date = request.form.get("date", "").strip() or date.today().isoformat()

    if not base_rate:
        flash("Please enter today's base rate.", "warning")
        return redirect(url_for("admin_dashboard"))

    try:
        base_rate_value = float(base_rate)
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO milk_rates (date, base_rate)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE base_rate = VALUES(base_rate)
            """,
            (rate_date, base_rate_value),
        )
        connection.commit()
        cursor.close()
        connection.close()
        flash(f"Milk base rate saved for {rate_date}: Rs. {base_rate_value:.2f}", "success")
    except ValueError:
        flash("Base rate must be a valid number.", "danger")
    except mysql.connector.Error as error:
        flash(f"Could not save milk rate: {error}", "danger")

    return redirect(url_for("admin_dashboard"))



@app.route("/farmers")
@login_required(["admin"])
def farmers_list():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Advanced stats per farmer for the management list
    cursor.execute("""
        SELECT 
            u.id, u.name, u.phone, u.village, u.join_date,
            COALESCE(SUM(m.litres), 0) as total_milk_month,
            COALESCE(AVG(m.fat), 0) as avg_fat,
            COALESCE(MAX(m.date), 'Never') as last_supply
        FROM users u
        LEFT JOIN milk_records m ON u.id = m.farmer_id AND MONTH(m.date) = MONTH(CURDATE()) AND YEAR(m.date) = YEAR(CURDATE())
        WHERE u.role = 'farmer'
        GROUP BY u.id
        ORDER BY u.name ASC
    """)
    farmers = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return render_template("farmers.html", user=current_user(), farmers=farmers)


@app.route("/admin_dashboard")
@login_required(["admin"])
def admin_dashboard():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT id, name, phone, village
        FROM users
        WHERE role = 'farmer'
        ORDER BY name ASC
        """
    )
    farmers = cursor.fetchall()
    
    # ... rest of the existing code ...

    # Smart Search Logic
    search_query = request.args.get("search", "").strip()
    where_clause = ""
    params = []
    if search_query:
        where_clause = " WHERE users.name LIKE %s OR users.phone LIKE %s OR users.village LIKE %s"
        params = [f"%{search_query}%", f"%{search_query}%", f"%{search_query}%"]

    cursor.execute(
        f"""
        SELECT
            milk_records.id,
            milk_records.date,
            milk_records.`session` AS session,
            milk_records.litres,
            milk_records.fat,
            milk_records.snf,
            milk_records.water_percent,
            milk_records.base_rate,
            milk_records.deduction,
            milk_records.final_rate,
            milk_records.amount,
            users.name AS farmer_name,
            users.phone AS farmer_phone,
            users.village
        FROM milk_records
        JOIN users ON users.id = milk_records.farmer_id
        {where_clause}
        ORDER BY milk_records.date DESC, milk_records.id DESC
        LIMIT 100
        """,
        params
    )
    records = cursor.fetchall()

    totals = get_dashboard_totals(cursor)
    current_rate = get_rate_for_date(cursor, date.today().isoformat())
    
    # New Smart Features
    rankings = get_farmer_ranking(cursor)
    alerts = get_missed_supply_alerts(cursor)
    weather = get_weather_insight()

    # Today's Stats
    cursor.execute("""
        SELECT 
            COALESCE(SUM(litres), 0) as litres,
            COALESCE(SUM(amount), 0) as revenue,
            COUNT(DISTINCT farmer_id) as farmers
        FROM milk_records
        WHERE date = CURDATE()
    """)
    today_stats = cursor.fetchone()

    # Yesterday's Stats
    cursor.execute("""
        SELECT 
            COALESCE(SUM(litres), 0) as litres,
            COALESCE(SUM(amount), 0) as revenue,
            COUNT(DISTINCT farmer_id) as farmers
        FROM milk_records
        WHERE date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    """)
    yesterday_stats = cursor.fetchone()

    # Today's Poor Quality Count
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM milk_records
        WHERE date = CURDATE() AND (fat < 3.0 OR water_percent > 15.0)
    """)
    poor_quality_today = cursor.fetchone()["count"]

    # Today's Suspicious Count (Deductions mean something went wrong)
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM milk_records
        WHERE date = CURDATE() AND deduction > 0
    """)
    suspicious_today = cursor.fetchone()["count"]

    cursor.close()
    connection.close()

    return render_template(
        "admin_dashboard.html",
        user=current_user(),
        farmers=farmers,
        today_stats=today_stats,
        yesterday_stats=yesterday_stats,
        poor_quality_today=poor_quality_today,
        suspicious_today=suspicious_today,
        weather=weather,
        search_query=search_query,
        totals=totals, # Keep for compatibility if needed, but we use new ones
        rankings=rankings,
        missed_alerts=alerts
    )
@app.route("/api/admin_stats")
@login_required(["admin"])
def api_admin_stats():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # 1. Volume Trend (14 Days)
    cursor.execute("""
        SELECT date, SUM(litres) as total_litres
        FROM milk_records
        GROUP BY date
        ORDER BY date DESC
        LIMIT 14
    """)
    daily_records = cursor.fetchall()
    daily_records.reverse()
    
    dates = [str(r['date']) for r in daily_records]
    daily_totals = [float(r['total_litres']) for r in daily_records]

    # 2. Revenue Trend (7 Days)
    cursor.execute("""
        SELECT date, SUM(amount) as revenue
        FROM milk_records
        GROUP BY date
        ORDER BY date DESC
        LIMIT 7
    """)
    rev_records = cursor.fetchall()
    rev_records.reverse()
    rev_labels = [str(r['date']) for r in rev_records]
    rev_data = [float(r['revenue']) for r in rev_records]

    # 3. Quality Distribution (Today)
    quality_dist = {"Excellent": 0, "Good": 0, "Average": 0, "Poor": 0}
    
    # Count Excellent
    cursor.execute("SELECT COUNT(*) as count FROM milk_records WHERE date = CURDATE() AND fat >= 4.0 AND water_percent <= 5.0")
    quality_dist["Excellent"] = cursor.fetchone()["count"]
    
    # Count Good
    cursor.execute("SELECT COUNT(*) as count FROM milk_records WHERE date = CURDATE() AND ((fat >= 3.5 AND fat < 4.0 AND water_percent <= 10.0) OR (fat >= 4.0 AND water_percent > 5.0 AND water_percent <= 10.0))")
    quality_dist["Good"] = cursor.fetchone()["count"]
    
    # Count Average
    cursor.execute("SELECT COUNT(*) as count FROM milk_records WHERE date = CURDATE() AND ((fat >= 3.0 AND fat < 3.5 AND water_percent <= 15.0) OR (fat >= 3.5 AND water_percent > 10.0 AND water_percent <= 15.0))")
    quality_dist["Average"] = cursor.fetchone()["count"]
    
    # Count Poor
    cursor.execute("SELECT COUNT(*) as count FROM milk_records WHERE date = CURDATE() AND (fat < 3.0 OR water_percent > 15.0)")
    quality_dist["Poor"] = cursor.fetchone()["count"]

    # 4. Today vs Yesterday (Full Year check for scale)
    cursor.execute("SELECT SUM(litres) as today FROM milk_records WHERE date = CURDATE()")
    v_today = float(cursor.fetchone()["today"] or 0)
    
    cursor.execute("SELECT SUM(litres) as yesterday FROM milk_records WHERE date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
    v_yesterday = float(cursor.fetchone()["yesterday"] or 0)

    # 5. Global Stats
    cursor.execute("SELECT COUNT(*) as farmer_count FROM users WHERE role = 'farmer'")
    f_count = cursor.fetchone()['farmer_count']

    cursor.close()
    connection.close()

    return jsonify({
        "daily_milk": {
            "dates": dates,
            "totals": daily_totals
        },
        "revenue_7d": {
            "labels": rev_labels,
            "data": rev_data
        },
        "quality_dist": quality_dist,
        "comparison": {
            "today": v_today,
            "yesterday": v_yesterday
        },
        "global_stats": {
            "total_farmers": f_count,
            "total_litres_collected": float(sum(daily_totals))
        }
    })


@app.route("/farmer_dashboard/<int:farmer_id>")
@login_required(["farmer", "admin"])
def farmer_dashboard(farmer_id):
    user = current_user()

    if user["role"] == "farmer" and user["id"] != farmer_id:
        flash("You can view only your own milk records.", "danger")
        return redirect(url_for("farmer_dashboard", farmer_id=user["id"]))

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT id, name, phone, village
        FROM users
        WHERE id = %s AND role = 'farmer'
        """,
        (farmer_id,),
    )
    farmer = cursor.fetchone()

    if not farmer:
        cursor.close()
        connection.close()
        flash("Farmer not found.", "danger")
        return redirect_to_role_dashboard(user)

    cursor.execute(
        """
        SELECT date, `session` AS session, litres, fat, snf, water_percent, base_rate, deduction, final_rate, amount
        FROM milk_records
        WHERE farmer_id = %s
        ORDER BY date DESC, id DESC
        """,
        (farmer_id,),
    )
    records = cursor.fetchall()

    totals = get_dashboard_totals(cursor, "AND farmer_id = %s", (farmer_id,))
    
    # New Farmer Features
    streak = get_supply_streak(cursor, farmer_id)
    weather = get_weather_insight()
    
    cursor.execute("SELECT * FROM ml_predictions WHERE farmer_id = %s ORDER BY date DESC LIMIT 1", (farmer_id,))
    ai_insight = cursor.fetchone()

    cursor.close()
    connection.close()

    return render_template(
        "farmer_dashboard.html",
        user=user,
        farmer=farmer,
        records=records,
        totals=totals,
        streak=streak,
        weather=weather,
        ai=ai_insight
    )


@app.route("/add_farmer", methods=["GET", "POST"])
@login_required(["admin"])
def add_farmer():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        village = request.form.get("village", "").strip()
        join_date = request.form.get("join_date", "").strip() or date.today().isoformat()
        telegram_chat_id = request.form.get("telegram_chat_id", "").strip() or None

        if not name or not phone:
            flash("Please fill all fields.", "warning")
            return redirect(url_for("add_farmer"))

        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO users (name, phone, role, village, join_date, telegram_chat_id)
                VALUES (%s, %s, 'farmer', %s, %s, %s)
                """,
                (name, phone, village, join_date, telegram_chat_id),
            )
            connection.commit()
            cursor.close()
            connection.close()
            flash("Farmer added successfully.", "success")
            return redirect(url_for("add_farmer"))
        except mysql.connector.IntegrityError:
            flash("This mobile number is already registered.", "danger")
        except mysql.connector.Error as error:
            flash(f"Could not add farmer: {error}", "danger")

    return render_template("add_farmer.html", user=current_user())


@app.route("/add_milk", methods=["GET", "POST"])
@login_required(["admin"])
def add_milk():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    if request.method == "POST":
        farmer_id = request.form.get("farmer_id", "").strip()
        record_date = request.form.get("date", "").strip() or date.today().isoformat()
        milk_session = request.form.get("session", "FN").strip()
        litres = request.form.get("litres", "").strip()
        fat = request.form.get("fat", "").strip()
        snf = request.form.get("snf", "").strip() or None
        water_percent = request.form.get("water_percent", "").strip() or None

        if not farmer_id or not litres or not fat or milk_session not in ("FN", "AN"):
            flash("Please select a farmer, session, litres, and fat.", "warning")
            cursor.close()
            connection.close()
            return redirect(url_for("add_milk"))

        try:
            litres_value = float(litres)
            fat_value = float(fat)

            cursor.execute(
                """
                SELECT id
                FROM users
                WHERE id = %s AND role = 'farmer'
                """,
                (farmer_id,),
            )
            farmer = cursor.fetchone()

            if not farmer:
                flash("Selected farmer was not found.", "danger")
            else:
                rate = get_rate_for_date(cursor, record_date)
                if not rate:
                    flash(f"Please ask admin to set the milk rate for {record_date}.", "warning")
                    cursor.close()
                    connection.close()
                    return redirect(url_for("add_milk"))

                quality = get_quality_rating(fat_value, water_percent)
                pricing = calculate_pricing(litres_value, fat_value, water_percent, rate["base_rate"], quality)
                
                # Get dynamic AI recommendation
                ai_rec = ai_engine.get_smart_recommendation({
                    "fat": fat_value,
                    "water_percent": water_percent,
                    "ph": request.form.get("ph"),
                    "temperature": request.form.get("temperature")
                }, quality)

                cursor.execute(
                    """
                    INSERT INTO milk_records
                        (farmer_id, date, `session`, litres, fat, snf, water_percent, base_rate, deduction, final_rate, amount, deduction_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        farmer_id,
                        record_date,
                        milk_session,
                        litres_value,
                        fat_value,
                        snf,
                        water_percent,
                        pricing["base_rate"],
                        pricing["deduction"],
                        pricing["final_rate"],
                        pricing["amount"],
                        pricing["deduction_reason"]
                    ),
                )
                
                # Update ml_predictions with the dynamic recommendation for this record
                # Note: ml_predictions usually stores daily forecasts, but we can update the recommendation based on the latest record
                cursor.execute("""
                    INSERT INTO ml_predictions (farmer_id, date, quality_prediction, recommendation)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    quality_prediction = VALUES(quality_prediction),
                    recommendation = VALUES(recommendation)
                """, (farmer_id, record_date, quality, ai_rec))

                connection.commit()
                
                cursor.execute("SELECT telegram_chat_id, language FROM users WHERE id = %s", (farmer_id,))
                user_info = cursor.fetchone()
                if user_info and user_info.get('telegram_chat_id'):
                    notifications.notify_instant_entry(user_info['telegram_chat_id'], user_info.get('language', 'en'), {
                        "date": record_date,
                        "session": milk_session,
                        "litres": litres_value,
                        "fat": fat_value,
                        "snf": snf,
                        "water_percent": water_percent,
                        "rate": pricing["final_rate"],
                        "amount": pricing["amount"]
                    })
                
                flash(
                    f"Milk entry added. Rate: Rs. {pricing['final_rate']:.2f}, Amount: Rs. {pricing['amount']:.2f}",
                    "success",
                )

            cursor.close()
            connection.close()
            return redirect(url_for("add_milk"))
        except ValueError:
            flash("Litres and fat must be valid numbers.", "danger")
        except mysql.connector.Error as error:
            flash(f"Could not add milk entry: {error}", "danger")

    cursor.execute(
        """
        SELECT id, name, phone, village
        FROM users
        WHERE role = 'farmer'
        ORDER BY name
        """
    )
    farmers = cursor.fetchall()
    current_rate = get_rate_for_date(cursor, date.today().isoformat())

    cursor.close()
    connection.close()

    return render_template(
        "add_milk.html",
        user=current_user(),
        farmers=farmers,
        today=date.today().isoformat(),
        current_rate=current_rate,
    )


def get_monthly_data(farmer_id, month, year):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    cursor.execute("SELECT id, name, phone, village, join_date FROM users WHERE id = %s AND role = 'farmer'", (farmer_id,))
    farmer = cursor.fetchone()
    if not farmer:
        cursor.close()
        connection.close()
        return None, None, None

    cursor.execute("""
        SELECT date, `session` AS session, litres, fat, snf, water_percent, base_rate, deduction, final_rate, amount
        FROM milk_records
        WHERE farmer_id = %s AND MONTH(date) = %s AND YEAR(date) = %s
        ORDER BY date ASC, id ASC
    """, (farmer_id, month, year))
    records = cursor.fetchall()

    cursor.execute("""
        SELECT COALESCE(SUM(litres), 0) as total_litres, COALESCE(SUM(amount), 0) as total_amount
        FROM milk_records
        WHERE farmer_id = %s AND MONTH(date) = %s AND YEAR(date) = %s
    """, (farmer_id, month, year))
    totals = cursor.fetchone()

    cursor.close()
    connection.close()
    return farmer, records, totals


@app.route("/monthly_report/<int:farmer_id>")
@login_required(["farmer", "admin"])
def monthly_report(farmer_id):
    user = current_user()
    if user["role"] == "farmer" and user["id"] != farmer_id:
        flash("You can view only your own milk records.", "danger")
        return redirect(url_for("farmer_dashboard", farmer_id=user["id"]))

    from datetime import date
    today = date.today()
    month = request.args.get("month", today.month, type=int)
    year = request.args.get("year", today.year, type=int)

    farmer, records, totals = get_monthly_data(farmer_id, month, year)
    if not farmer:
        flash("Farmer not found.", "danger")
        return redirect_to_role_dashboard(user)

    years = range(today.year - 2, today.year + 1)
    months = [{"val": i, "name": date(1900, i, 1).strftime('%B')} for i in range(1, 13)]

    return render_template(
        "monthly_report.html",
        user=user,
        farmer=farmer,
        records=records,
        totals=totals,
        selected_month=month,
        selected_year=year,
        months=months,
        years=years
    )


@app.route("/monthly_report/<int:farmer_id>/pdf")
@login_required(["farmer", "admin", "operator"])
def monthly_report_pdf(farmer_id):
    user = current_user()
    if user["role"] == "farmer" and user["id"] != farmer_id:
        flash("You can download only your own milk records.", "danger")
        return redirect(url_for("farmer_dashboard", farmer_id=user["id"]))

    from datetime import date
    today = date.today()
    month = request.args.get("month", today.month, type=int)
    year = request.args.get("year", today.year, type=int)

    farmer, records, totals = get_monthly_data(farmer_id, month, year)
    if not farmer:
        flash("Farmer not found.", "danger")
        return redirect_to_role_dashboard(user)

    month_name = date(1900, month, 1).strftime('%B')

    import io
    from fpdf import FPDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 16)
    pdf.cell(0, 10, "Monthly Milk Collection Report", new_x="LMARGIN", new_y="NEXT", align="C")
    
    pdf.set_font("helvetica", "", 12)
    pdf.ln(5)
    pdf.cell(0, 8, f"Farmer Name: {farmer['name']}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Phone: {farmer['phone']} | Village: {farmer.get('village') or 'N/A'}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Month/Year: {month_name} {year}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)

    pdf.set_font("helvetica", "B", 10)
    col_widths = [30, 25, 25, 25, 30, 35]
    headers = ["Date", "Session", "Litres", "Fat", "Rate (Rs)", "Amount (Rs)"]
    for i, h in enumerate(headers):
        pdf.cell(col_widths[i], 10, h, border=1, align="C")
    pdf.ln()

    pdf.set_font("helvetica", "", 10)
    for r in records:
        pdf.cell(col_widths[0], 10, str(r['date']), border=1, align="C")
        pdf.cell(col_widths[1], 10, "Morning" if r['session'] == "FN" else "Evening", border=1, align="C")
        pdf.cell(col_widths[2], 10, f"{float(r['litres']):.2f}", border=1, align="C")
        pdf.cell(col_widths[3], 10, f"{float(r['fat']):.2f}", border=1, align="C")
        pdf.cell(col_widths[4], 10, f"{float(r['final_rate']):.2f}", border=1, align="C")
        pdf.cell(col_widths[5], 10, f"{float(r['amount']):.2f}", border=1, align="C")
        pdf.ln()

    pdf.ln(5)
    pdf.set_font("helvetica", "B", 12)
    pdf.cell(0, 10, f"Total Litres: {float(totals['total_litres']):.2f} L", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 10, f"Total Amount: Rs. {float(totals['total_amount']):.2f}", new_x="LMARGIN", new_y="NEXT")

    pdf_bytes = pdf.output()
    output_stream = io.BytesIO(pdf_bytes)

    return send_file(
        output_stream,
        as_attachment=True,
        download_name=f"milk_report_{farmer['name'].replace(' ', '_')}_{month}_{year}.pdf",
        mimetype="application/pdf"
    )

@app.route("/api/milk_entry", methods=["POST"])
def api_milk_entry():
    data = request.get_json(silent=True) or {}
    farmer_id = data.get("farmer_id")
    litres = data.get("litres")
    fat = data.get("fat")
    record_date = data.get("date", date.today().isoformat())
    milk_session = data.get("session", "FN")

    if not farmer_id or litres is None or fat is None:
        return jsonify({"error": "farmer_id, litres, and fat are required"}), 400

    if milk_session not in ("FN", "AN"):
        return jsonify({"error": "session must be FN or AN"}), 400

    try:
        litres_value = float(litres)
        fat_value = float(fat)

        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id, name
            FROM users
            WHERE id = %s AND role = 'farmer'
            """,
            (farmer_id,),
        )
        farmer = cursor.fetchone()

        if not farmer:
            cursor.close()
            connection.close()
            return jsonify({"error": "Farmer not found"}), 404

        rate = get_rate_for_date(cursor, record_date)
        if not rate:
            cursor.close()
            connection.close()
            return jsonify({"error": f"Milk rate is not set for {record_date}"}), 400

        quality = get_quality_rating(litres_value, fat_value) # Assuming fat/water logic
        pricing = calculate_pricing(litres_value, fat_value, 0, rate["base_rate"], quality)

        cursor.execute(
            """
            INSERT INTO milk_records
                (farmer_id, date, `session`, litres, fat, base_rate, deduction, final_rate, amount, deduction_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                farmer_id,
                record_date,
                milk_session,
                litres_value,
                fat_value,
                pricing["base_rate"],
                pricing["deduction"],
                pricing["final_rate"],
                pricing["amount"],
                pricing["deduction_reason"]
            ),
        )
        connection.commit()
        record_id = cursor.lastrowid
        
        cursor.execute("SELECT telegram_chat_id FROM users WHERE id = %s", (farmer_id,))
        chat_id_row = cursor.fetchone()
        if chat_id_row and chat_id_row.get('telegram_chat_id'):
            send_telegram_notification(chat_id_row['telegram_chat_id'], {
                "date": record_date,
                "session": milk_session,
                "litres": litres_value,
                "fat": fat_value,
                "amount": pricing["amount"]
            })

        cursor.close()
        connection.close()

        return jsonify(
            {
                "message": "Milk entry added successfully",
                "record": {
                    "id": record_id,
                    "farmer_id": int(farmer_id),
                    "farmer_name": farmer["name"],
                    "date": record_date,
                    "session": milk_session,
                    "litres": litres_value,
                    "fat": fat_value,
                    "base_rate": pricing["base_rate"],
                    "deduction": pricing["deduction"],
                    "final_rate": pricing["final_rate"],
                    "amount": pricing["amount"],
                },
            }
        ), 201
    except ValueError:
        return jsonify({"error": "litres and fat must be numbers"}), 400
    except mysql.connector.Error as error:
        return jsonify({"error": str(error)}), 500

@app.route("/api/machine_entry", methods=["POST"])
def api_machine_entry():
    data = request.get_json(silent=True) or {}
    farmer_id = data.get("farmer_id")
    litres = data.get("litres")
    fat = data.get("fat")
    snf = data.get("snf")
    water_percent = data.get("water_percent")
    record_date = data.get("date", date.today().isoformat())
    milk_session = data.get("session", "FN")

    if not farmer_id or litres is None or fat is None:
        return jsonify({"error": "farmer_id, litres, and fat are required"}), 400

    if milk_session not in ("FN", "AN"):
        return jsonify({"error": "session must be FN or AN"}), 400

    try:
        litres_value = float(litres)
        fat_value = float(fat)
        snf_value = float(snf) if snf is not None else None
        water_percent_value = float(water_percent) if water_percent is not None else None

        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id, name
            FROM users
            WHERE id = %s AND role = 'farmer'
            """,
            (farmer_id,),
        )
        farmer = cursor.fetchone()

        if not farmer:
            cursor.close()
            connection.close()
            return jsonify({"error": "Farmer not found"}), 404

        rate = get_rate_for_date(cursor, record_date)
        if not rate:
            cursor.close()
            connection.close()
            return jsonify({"error": f"Milk rate is not set for {record_date}"}), 400

        quality = get_quality_rating(fat_value, water_percent_value)
        pricing = calculate_pricing(litres_value, fat_value, water_percent_value, rate["base_rate"], quality)

        cursor.execute(
            """
            INSERT INTO milk_records
                (farmer_id, date, `session`, litres, fat, snf, water_percent, base_rate, deduction, final_rate, amount, deduction_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                farmer_id,
                record_date,
                milk_session,
                litres_value,
                fat_value,
                snf_value,
                water_percent_value,
                pricing["base_rate"],
                pricing["deduction"],
                pricing["final_rate"],
                pricing["amount"],
                pricing["deduction_reason"]
            ),
        )
        connection.commit()
        record_id = cursor.lastrowid
        
        cursor.execute("SELECT telegram_chat_id FROM users WHERE id = %s", (farmer_id,))
        chat_id_row = cursor.fetchone()
        if chat_id_row and chat_id_row.get('telegram_chat_id'):
            send_telegram_notification(chat_id_row['telegram_chat_id'], {
                "date": record_date,
                "session": milk_session,
                "litres": litres_value,
                "fat": fat_value,
                "amount": pricing["amount"]
            })

        cursor.close()
        connection.close()

        return jsonify(
            {
                "message": "Machine entry added successfully",
                "record": {
                    "id": record_id,
                    "farmer_id": int(farmer_id),
                    "farmer_name": farmer["name"],
                    "date": record_date,
                    "session": milk_session,
                    "litres": litres_value,
                    "fat": fat_value,
                    "snf": snf_value,
                    "water_percent": water_percent_value,
                    "base_rate": pricing["base_rate"],
                    "deduction": pricing["deduction"],
                    "final_rate": pricing["final_rate"],
                    "amount": pricing["amount"],
                },
            }
        ), 201
    except ValueError:
        return jsonify({"error": "numeric values required"}), 400
    except mysql.connector.Error as error:
        return jsonify({"error": str(error)}), 500

@app.route("/reports")
@login_required(["admin", "operator"])
def reports():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # Get filter params
    start_date = request.args.get("start_date", date.today().replace(day=1).isoformat())
    end_date = request.args.get("end_date", date.today().isoformat())
    farmer_id = request.args.get("farmer_id", "")
    quality = request.args.get("quality", "")
    session_filter = request.args.get("session", "")

    # Base Query
    sql = """
        SELECT m.*, u.name as farmer_name, u.phone as farmer_phone, u.village
        FROM milk_records m
        JOIN users u ON m.farmer_id = u.id
        WHERE m.date BETWEEN %s AND %s
    """
    params = [start_date, end_date]

    if farmer_id:
        sql += " AND m.farmer_id = %s"
        params.append(farmer_id)
    
    if session_filter:
        sql += " AND m.session = %s"
        params.append(session_filter)

    if quality:
        if quality == "Excellent":
            sql += " AND m.fat >= 4.0 AND m.water_percent <= 5.0"
        elif quality == "Good":
            sql += " AND ((m.fat >= 3.5 AND m.fat < 4.0 AND m.water_percent <= 10.0) OR (m.fat >= 4.0 AND m.water_percent > 5.0 AND m.water_percent <= 10.0))"
        elif quality == "Average":
            sql += " AND ((m.fat >= 3.0 AND m.fat < 3.5 AND m.water_percent <= 15.0) OR (m.fat >= 3.5 AND m.water_percent > 10.0 AND m.water_percent <= 15.0))"
        elif quality == "Poor":
            sql += " AND (m.fat < 3.0 OR m.water_percent > 15.0)"

    sql += " ORDER BY m.date DESC, m.id DESC"
    
    cursor.execute(sql, params)
    records = cursor.fetchall()

    # Calculate Totals
    total_litres = sum(r['litres'] for r in records)
    total_amount = sum(r['amount'] for r in records)

    # Get all farmers for filter dropdown
    cursor.execute("SELECT id, name FROM users WHERE role = 'farmer' ORDER BY name ASC")
    farmers = cursor.fetchall()

    cursor.close()
    connection.close()

    return render_template(
        "reports.html",
        records=records,
        farmers=farmers,
        total_litres=total_litres,
        total_amount=total_amount,
        filters={
            "start_date": start_date,
            "end_date": end_date,
            "farmer_id": farmer_id,
            "quality": quality,
            "session": session_filter
        }
    )

    return render_template("bulk_upload.html", user=current_user(), results=results)

# Global buffer for prediction data to avoid file I/O issues
PREDICTION_DATA_BUFFER = {}

@app.route("/upload_predict", methods=["GET", "POST"])
@login_required(["admin"])
def upload_predict():
    user = current_user()
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("No file selected.", "warning")
            return redirect(url_for("upload_predict"))

        filename = file.filename.lower()
        try:
            if filename.endswith(".csv"):
                df = pd.read_csv(file)
            elif filename.endswith((".xlsx", ".xls")):
                df = pd.read_excel(file)
            elif filename.endswith(".pdf"):
                df = extract_data_from_pdf(file)
            else:
                flash("Unsupported file format.", "danger")
                return redirect(url_for("upload_predict"))

            if df is not None:
                # Basic cleaning
                df.columns = [str(c).strip().lower() for c in df.columns]
                
                # Store in memory buffer instead of file to ensure stability
                user_id = user.get('id', 'default')
                PREDICTION_DATA_BUFFER[user_id] = df.to_json(orient="records")
                
                session['predict_ready'] = True
                session['predict_count'] = len(df)
                flash(f"Loaded {len(df)} records. Ready for AI analysis.", "success")
                return render_template("upload_predict.html", user=user, data=df.to_dict(orient="records"))
        except Exception as e:
            flash(f"Error loading file: {str(e)}", "danger")
            return redirect(url_for("upload_predict"))

    # Cleanup memory on fresh visit
    user_id = user.get('id', 'default')
    PREDICTION_DATA_BUFFER.pop(user_id, None)
    session.pop('predict_ready', None)
    session.pop('predict_count', None)
    
    return render_template("upload_predict.html", user=user, data=None)

@app.route("/api/predict_batch", methods=["POST"])
@login_required(["admin"])
def api_predict_batch():
    import json
    user = current_user()
    user_id = user.get('id', 'default')
    
    data_json = PREDICTION_DATA_BUFFER.get(user_id)
    if not data_json:
        return jsonify({"error": "No data found to predict. Please upload again."}), 400
    
    try:
        # Load into DF and perform a deep clean for JSON serialization
        df = pd.read_json(io.StringIO(data_json), orient='records')
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
        
        # Replace all NaN/Inf with None (standard JSON null)
        df = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)
        
        print(f"AI Hub: Processing {len(df)} records for user {user_id}")
        results = []
        
        # Pre-clean all numerical columns for the engine
        num_cols = ['fat', 'snf', 'water_percent', 'ph', 'temperature', 'litres']
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: ai_engine._clean_val(x))
            else:
                # Add default column if missing
                default_val = 8.5 if col == 'snf' else (6.6 if col == 'ph' else (35.0 if col == 'temperature' else 0.0))
                df[col] = default_val

        # Efficient row processing
        for row in df.to_dict(orient='records'):
            fat, snf, water = row.get('fat', 0), row.get('snf', 8.5), row.get('water_percent', 0)
            ph, temp, litres = row.get('ph', 6.6), row.get('temperature', 35.0), row.get('litres', 0)

            quality_score = ai_engine.calculate_quality_score(fat, snf, water)
            quality_grade = get_quality_rating(fat, water)
            fraud_risk = ai_engine.detect_fraud(litres, fat, ph, temp, water)
            recommendation = ai_engine.get_smart_recommendation(row, quality_grade)
            
            row.update({
                "quality_score": quality_score, "quality_grade": quality_grade,
                "fraud_risk": fraud_risk, "recommendation": recommendation
            })
            results.append(row)
            
        # Final Serialization Safety: Convert any remaining NumPy types/NaN to Python-native types
        safe_results = []
        for res in results:
            clean_res = {}
            for k, v in res.items():
                if pd.isna(v): clean_res[k] = None
                elif isinstance(v, (np.float64, np.float32)): clean_res[k] = float(v)
                elif isinstance(v, (np.int64, np.int32)): clean_res[k] = int(v)
                else: clean_res[k] = v
            safe_results.append(clean_res)
            
        print(f"AI Hub: Successfully processed {len(safe_results)} results.")
        return jsonify({"results": safe_results})

    except Exception as e:
        import traceback
        error_info = {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "status": "fail"
        }
        return jsonify(error_info), 500


def extract_data_from_pdf(file):
    """Try to extract table data from PDF using pdfplumber."""
    try:
        data = []
        with pdfplumber.open(file) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    # Assume first row is header
                    headers = table[0]
                    for row in table[1:]:
                        if len(row) == len(headers):
                            data.append(dict(zip(headers, row)))
        
        if not data:
            return None
            
        return pd.DataFrame(data)
    except Exception:
        return None


def process_bulk_data(df):
    """Validate and insert bulk milk records."""
    required_cols = ["farmer_phone", "date", "session", "litres", "fat"]
    # Clean column names
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # Check for missing required columns
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    results = {
        "total": len(df),
        "inserted": 0,
        "failed": 0,
        "failed_rows": [],
        "summary": {
            "total_litres": 0,
            "avg_fat": 0,
            "good_quality": 0,
            "poor_quality": 0
        }
    }
    
    fat_sum = 0
    
    for index, row in df.iterrows():
        row_num = index + 2  # Adjust for 1-based index and header
        phone = str(row.get("farmer_phone", "")).strip()
        record_date = str(row.get("date", "")).strip()
        session_val = str(row.get("session", "FN")).strip().upper()
        
        try:
            litres = float(row.get("litres", 0))
            fat = float(row.get("fat", 0))
            snf = row.get("snf")
            water_percent = row.get("water_percent")
            
            # Validation
            if not phone: raise ValueError("Phone missing")
            if not record_date: raise ValueError("Date missing")
            if session_val not in ("FN", "AN"): raise ValueError("Invalid session (use FN/AN)")

            # Check Farmer
            cursor.execute("SELECT id FROM users WHERE phone = %s AND role = 'farmer'", (phone,))
            farmer = cursor.fetchone()
            if not farmer:
                raise ValueError(f"Farmer phone {phone} not found")

            # Check Rate
            rate_row = get_rate_for_date(cursor, record_date)
            if not rate_row:
                raise ValueError(f"No base rate set for {record_date}")

            # Calculate Pricing
            quality = get_quality_rating(fat, water_percent)
            pricing = calculate_pricing(litres, fat, water_percent, rate_row["base_rate"], quality)
            
            # Insert
            cursor.execute(
                """
                INSERT INTO milk_records 
                (farmer_id, date, `session`, litres, fat, snf, water_percent, base_rate, deduction, final_rate, amount, deduction_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (farmer["id"], record_date, session_val, litres, fat, snf, water_percent, 
                 pricing["base_rate"], pricing["deduction"], pricing["final_rate"], pricing["amount"], pricing["deduction_reason"])
            )
            
            results["inserted"] += 1
            results["summary"]["total_litres"] += litres
            fat_sum += fat
            
            # Get quality for summary
            quality = get_quality_rating(fat, water_percent)
            if quality in ("Excellent", "Good"):
                results["summary"]["good_quality"] += 1
            else:
                results["summary"]["poor_quality"] += 1

        except Exception as e:
            results["failed"] += 1
            results["failed_rows"].append({
                "row_number": row_num,
                "phone": phone,
                "reason": str(e)
            })

    if results["inserted"] > 0:
        results["summary"]["avg_fat"] = fat_sum / results["inserted"]
        connection.commit()
        
    cursor.close()
    connection.close()
    return results


@app.route("/download_template/<file_type>")
@login_required(["admin"])
def download_template(file_type):
    from datetime import date
    headers = ["farmer_phone", "date", "session", "litres", "fat", "snf", "water_percent"]
    sample_data = [
        ["9876543210", date.today().isoformat(), "FN", 10.5, 4.2, 8.5, 2],
        ["8888877777", date.today().isoformat(), "AN", 8.2, 3.8, 8.2, 5]
    ]
    
    df = pd.DataFrame(sample_data, columns=headers)
    
    if file_type == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False)
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype="text/csv",
            as_attachment=True,
            download_name="milk_data_template.csv"
        )
    elif file_type == "excel":
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="MilkData")
        output.seek(0)
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="milk_data_template.xlsx"
        )
    
    flash("Invalid template type.", "danger")
    return redirect(url_for("bulk_upload"))


@app.route("/export/<file_type>")
@login_required(["admin"])
def export_data(file_type):
    # Get filter params from args
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")
    farmer_id = request.args.get("farmer_id", "")
    quality = request.args.get("quality", "")
    session_filter = request.args.get("session", "")

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # Base Query
    sql = """
        SELECT 
            m.date, m.session, u.name as farmer_name, u.phone as farmer_phone,
            m.litres, m.fat, m.snf, m.water_percent,
            m.base_rate, m.deduction, m.final_rate, m.amount,
            m.deduction_reason
        FROM milk_records m
        JOIN users u ON m.farmer_id = u.id
        WHERE 1=1
    """
    params = []

    if start_date and end_date:
        sql += " AND m.date BETWEEN %s AND %s"
        params.extend([start_date, end_date])
    
    if farmer_id:
        sql += " AND m.farmer_id = %s"
        params.append(farmer_id)
    
    if session_filter:
        sql += " AND m.session = %s"
        params.append(session_filter)

    if quality:
        if quality == "Excellent":
            sql += " AND m.fat >= 4.0 AND m.water_percent <= 5.0"
        elif quality == "Good":
            sql += " AND ((m.fat >= 3.5 AND m.fat < 4.0 AND m.water_percent <= 10.0) OR (m.fat >= 4.0 AND m.water_percent > 5.0 AND m.water_percent <= 10.0))"
        elif quality == "Average":
            sql += " AND ((m.fat >= 3.0 AND m.fat < 3.5 AND m.water_percent <= 15.0) OR (m.fat >= 3.5 AND m.water_percent > 10.0 AND m.water_percent <= 15.0))"
        elif quality == "Poor":
            sql += " AND (m.fat < 3.0 OR m.water_percent > 15.0)"

    sql += " ORDER BY m.date DESC, m.id DESC"
    
    cursor.execute(sql, params)
    records = cursor.fetchall()
    cursor.close()
    connection.close()

    df = pd.DataFrame(records)
    
    if file_type == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False)
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"milk_records_{date.today()}.csv"
        )
    elif file_type == "excel":
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Records")
        output.seek(0)
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"milk_records_{date.today()}.xlsx"
        )
    elif file_type == "pdf":
        from fpdf import FPDF
        pdf = FPDF(orientation='L', unit='mm', format='A4')
        pdf.add_page()
        pdf.set_font("helvetica", "B", 16)
        pdf.cell(0, 10, "Global Milk Collection Report", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.set_font("helvetica", "", 10)
        pdf.cell(0, 10, f"Generated on: {date.today().strftime('%B %d, %Y')}", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(10)

        # Table Headers
        pdf.set_font("helvetica", "B", 9)
        pdf.set_fill_color(240, 240, 240)
        cols = [
            ("Date", 25), ("Farmer", 45), ("Litres", 20), ("Fat", 20), 
            ("SNF", 20), ("Water %", 20), ("B.Rate", 20), ("F.Rate", 20), ("Amount", 25)
        ]
        
        for col in cols:
            pdf.cell(col[1], 10, col[0], border=1, align="C", fill=True)
        pdf.ln()

        # Data Rows
        pdf.set_font("helvetica", "", 8)
        for r in records:
            pdf.cell(25, 8, str(r['date']), border=1, align="C")
            pdf.cell(45, 8, str(r['farmer_name'])[:25], border=1)
            pdf.cell(20, 8, f"{float(r['litres']):.1f} L", border=1, align="C")
            pdf.cell(20, 8, f"{float(r['fat']):.2f}%", border=1, align="C")
            pdf.cell(20, 8, f"{float(r['snf'] or 0):.2f}%", border=1, align="C")
            pdf.cell(20, 8, f"{float(r['water_percent'] or 0):.2f}%", border=1, align="C")
            pdf.cell(20, 8, f"Rs.{float(r['base_rate']):.1f}", border=1, align="C")
            pdf.cell(20, 8, f"Rs.{float(r['final_rate']):.1f}", border=1, align="C")
            pdf.cell(25, 8, f"Rs.{float(r['amount']):.1f}", border=1, align="R")
            pdf.ln()

        pdf_bytes = pdf.output()
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"global_milk_report_{date.today()}.pdf"
        )
    
    return redirect(url_for("admin_dashboard"))


@app.route("/train_models")
@login_required(["admin"])
def train_models():
    try:
        # Run training in a separate process
        subprocess.run(["python", "train_models.py"], check=True)
        
        # After training, update ml_predictions table for all farmers
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("SELECT id FROM users WHERE role = 'farmer'")
        farmers = cursor.fetchall()
        
        for farmer in farmers:
            update_miner_predictions(cursor, farmer['id'])
            
        connection.commit()
        cursor.close()
        connection.close()
        
        flash("AI models trained and predictions updated successfully!", "success")
    except Exception as e:
        flash(f"Error training models: {e}", "danger")
    
    return redirect(url_for("admin_dashboard"))


def update_miner_predictions(cursor, farmer_id):
    """Generate and store personalized ML predictions for a farmer."""
    cursor.execute("""
        SELECT litres, fat, snf, water_percent, ph, temperature, date 
        FROM milk_records 
        WHERE farmer_id = %s 
        ORDER BY date DESC
    """, (farmer_id,))
    records = cursor.fetchall()
    
    if len(records) < 1: return

    # Convert to DataFrame for better trend analysis
    df = pd.DataFrame(records)
    # Ensure numeric columns are floats (not Decimals from MySQL)
    for col in ['litres', 'fat', 'snf', 'water_percent', 'ph', 'temperature']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
    
    latest = records[0]
    
    # 1. Predictions
    pred_litres = ai_engine.predict_tomorrow(df)
    
    # 2. Quality & Scaling
    score = ai_engine.calculate_quality_score(latest['fat'], latest['snf'], latest['water_percent'])
    quality_grade = "High" if score >= 80 else "Medium" if score >= 60 else "Low"
    
    # 3. Fraud Detection (Personalized)
    fraud = ai_engine.detect_fraud(float(latest['litres']), float(latest['fat']), 
                                  float(latest.get('ph', 6.6) or 6.6), 
                                  float(latest.get('temperature', 35.0) or 35.0), 
                                  latest['water_percent'], history=df)
    
    # 4. Trend & Confidence
    trend = ai_engine.get_production_trend(df)
    conf = ai_engine.get_confidence_score(len(df))
    
    # 5. Personalized Recommendation
    rec = ai_engine.get_personalized_recommendation(score, trend, latest)

    # Insert/Update Predictions
    cursor.execute("""
        INSERT INTO ml_predictions 
        (farmer_id, date, predicted_litres, quality_prediction, fraud_risk, performance_score, recommendation, confidence_score, trend)
        VALUES (%s, CURDATE(), %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        predicted_litres = VALUES(predicted_litres),
        quality_prediction = VALUES(quality_prediction),
        fraud_risk = VALUES(fraud_risk),
        performance_score = VALUES(performance_score),
        recommendation = VALUES(recommendation),
        confidence_score = VALUES(confidence_score),
        trend = VALUES(trend)
    """, (farmer_id, pred_litres, quality_grade, fraud, score, rec, conf, trend))

    # Update Farmer Scores Table
    cursor.execute("""
        INSERT INTO farmer_scores (farmer_id, score, trend)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        score = VALUES(score),
        trend = VALUES(trend),
        updated_at = CURRENT_TIMESTAMP
    """, (farmer_id, score, trend))

    # Send Notification if High Risk or Poor Quality
    if fraud == 'High' or score < 40:
        cursor.execute("SELECT telegram_chat_id, name FROM users WHERE id = %s", (farmer_id,))
        user_info = cursor.fetchone()
        if user_info and user_info['telegram_chat_id']:
            alert_msg = f"🥛 *Milk AI Alert* for {user_info['name']}\n\n"
            if fraud == 'High':
                alert_msg += f"⚠️ *CRITICAL RISK DETECTED!*\n"
            else:
                alert_msg += f"📉 *Quality Alert*\n"
            alert_msg += f"\n*Details:* {rec}\n*Advice:* Please consult the dairy manager.\n\n_System Admin | Milk AI_"
            send_telegram_notification(user_info['telegram_chat_id'], alert_msg)


@app.route("/ai_admin")
@login_required(["admin"])
def ai_admin():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # 1. High Risk Fraud Alerts
    cursor.execute("""
        SELECT u.name, u.phone, p.* 
        FROM ml_predictions p
        JOIN users u ON p.farmer_id = u.id
        WHERE p.fraud_risk = 'High'
        ORDER BY p.date DESC
    """)
    suspicious = cursor.fetchall()
    
    # 2. Adulteration Analysis (Water Mixing Monitor)
    cursor.execute("""
        SELECT u.name, u.phone, p.quality_prediction, p.recommendation, m.water_percent, m.fat, m.ph, m.temperature, m.date
        FROM milk_records m
        JOIN users u ON m.farmer_id = u.id
        LEFT JOIN ml_predictions p ON m.farmer_id = p.farmer_id AND m.date = p.date
        WHERE m.water_percent > 5
        ORDER BY m.water_percent DESC
    """)
    water_alerts = cursor.fetchall()
    
    # Calculate Dynamic Fraud Score for Water Alerts
    for row in water_alerts:
        score = 0
        w = float(row.get('water_percent') or 0)
        f = float(row.get('fat') or 0)
        p = float(row.get('ph') or 0)
        t = float(row.get('temperature') or 0)
        
        if w > 5: score += 30
        if f < 3: score += 25
        if p < 6.3 or p > 6.8: score += 20
        if t > 15: score += 25
        
        row['fraud_score'] = min(100, score)
    
    if water_alerts:
        print(f"DEBUG: AI Admin Adulteration Row -> {water_alerts[0]}")

    # 3. Poor Quality Reports (AI Grade = Low/Medium)
    cursor.execute("""
        SELECT u.name, u.phone, p.recommendation, m.fat, m.ph, m.temperature, m.water_percent, m.date
        FROM milk_records m
        JOIN users u ON m.farmer_id = u.id
        LEFT JOIN ml_predictions p ON m.farmer_id = p.farmer_id AND m.date = p.date
        WHERE m.fat < 3 OR m.water_percent > 10 OR (p.id IS NOT NULL AND p.quality_prediction IN ('Low', 'low'))
        ORDER BY m.date DESC
    """)
    poor_quality = cursor.fetchall()

    # 4. Top Farmers
    cursor.execute("""
        SELECT u.name, p.performance_score, p.quality_prediction
        FROM ml_predictions p
        JOIN users u ON p.farmer_id = u.id
        WHERE p.date = (SELECT MAX(date) FROM ml_predictions)
        ORDER BY p.performance_score DESC
        LIMIT 5
    """)
    top_farmers = cursor.fetchall()
    
    cursor.close()
    connection.close()
    
    return render_template("ai_admin.html", 
                           user=current_user(), 
                           suspicious=suspicious, 
                           water_alerts=water_alerts,
                           poor_quality=poor_quality,
                           top_farmers=top_farmers)


@app.route("/api/ai_farmer_stats/<int:farmer_id>")
@login_required(["farmer", "admin"])
def api_ai_farmer_stats(farmer_id):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    cursor.execute("SELECT * FROM ml_predictions WHERE farmer_id = %s ORDER BY date DESC LIMIT 1", (farmer_id,))
    prediction = cursor.fetchone()
    
    cursor.close()
    connection.close()
    
    return jsonify(prediction or {})


@app.route("/delete_farmer/<int:farmer_id>")
@login_required(["admin"])
def delete_farmer(farmer_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        # Note: Foreign key is ON DELETE CASCADE, so records will be deleted automatically.
        cursor.execute("DELETE FROM users WHERE id = %s AND role = 'farmer'", (farmer_id,))
        connection.commit()
        flash("Farmer removed successfully.", "success")
    except Exception as e:
        flash(f"Error removing farmer: {e}", "danger")
    finally:
        cursor.close()
        connection.close()
    return redirect(url_for("farmers_list"))


if __name__ == "__main__":
    from scheduler import start_scheduler
    
    try:
        create_database_and_tables()
        
        # Start Automation Scheduler in background
        if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
            start_scheduler(blocking=False)
            
    except mysql.connector.Error as error:
        print()
        print("Could not connect to MySQL.")
        print(f"Tried host={DB_HOST}, port={DB_PORT}, user={DB_USER}")
        print("Make sure MySQL Server is installed and running.")
        print("If your MySQL root user has a password, set DB_PASSWORD before running.")
        print(f"Details: {error}")
        raise SystemExit(1)

    app.run(debug=True)
