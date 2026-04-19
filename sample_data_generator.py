import mysql.connector
import os
import random
from datetime import date
import requests

# Use environment variables or defaults from app.py
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Mmani2005@")
DB_NAME = os.getenv("DB_NAME", "milk_collection")

# Pricing Configuration (Matching app.py logic)
BASE_RATE_FALLBACK = 42.0

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME
    )

def get_quality_rating(fat, water):
    if fat >= 4.0 and water <= 5.0: return "Excellent"
    if fat >= 3.5 and water <= 10.0: return "Good"
    if fat >= 3.0 and water <= 15.0: return "Average"
    return "Poor"

def calculate_pricing(litres, fat, water, base_rate):
    quality = get_quality_rating(fat, water)
    final_rate = float(base_rate)
    reasons = []
    
    # 1. Fat Adjustment
    if fat >= 5.0: final_rate += 5; reasons.append(f"High fat bonus (+₹5)")
    elif fat >= 4.5: final_rate += 3; reasons.append(f"Fat bonus (+₹3)")
    
    # 2. Water Deduction
    if water > 20: final_rate -= 10; reasons.append(f"High water mixing (-₹10)")
    elif water > 10: final_rate -= 6; reasons.append(f"Water mixing detected (-₹6)")
    elif water > 5: final_rate -= 3; reasons.append(f"Minor water mixing (-₹3)")
    
    # 3. Quality Adjustment
    if quality == "Excellent": final_rate += 4; reasons.append(f"Excellent quality (+₹4)")
    elif quality == "Average": final_rate -= 2; reasons.append(f"Average quality (-₹2)")
    elif quality == "Poor": final_rate -= 5; reasons.append(f"Poor quality (-₹5)")
    
    final_rate = max(20.0, final_rate)
    amount = float(litres) * final_rate
    
    return {
        "base_rate": float(base_rate),
        "final_rate": final_rate,
        "amount": amount,
        "deduction": float(base_rate) - final_rate,
        "deduction_reason": " | ".join(reasons) if reasons else "Full Rate Applied"
    }

def generate_session_data(session_type="FN", target_date=None):
    if target_date is None:
        target_date = date.today()
        
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # 1. Get Base Rate for the date
    cursor.execute("SELECT base_rate FROM milk_rates WHERE date = %s", (target_date,))
    rate_row = cursor.fetchone()
    base_rate = rate_row['base_rate'] if rate_row else BASE_RATE_FALLBACK
    
    # 2. Get All Farmers
    cursor.execute("SELECT id, name FROM users WHERE role = 'farmer'")
    farmers = cursor.fetchall()
    
    count = 0
    for farmer in farmers:
        # 3. Skip check (Approx 20% skip probability)
        if random.random() < 0.2:
            print(f"Skipping {farmer['name']} for {session_type}")
            continue
            
        # 4. Duplicate check
        cursor.execute("SELECT id FROM milk_records WHERE farmer_id = %s AND date = %s AND session = %s",
                       (farmer['id'], target_date, session_type))
        if cursor.fetchone():
            continue
            
        # 5. Generate Stats
        litres = round(random.uniform(2.0, 10.0), 1)
        fat = round(random.uniform(2.5, 5.5), 1)
        snf = round(random.uniform(7.5, 9.0), 1)
        water = round(random.uniform(0.0, 20.0), 1)
        ph = round(random.uniform(6.0, 6.8), 1)
        temp = round(random.uniform(25.0, 35.0), 1)
        
        pricing = calculate_pricing(litres, fat, water, base_rate)
        
        # 6. Insert
        sql = """
            INSERT INTO milk_records 
            (farmer_id, date, session, litres, fat, snf, water_percent, ph, temperature, 
             base_rate, deduction, final_rate, amount, deduction_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            farmer['id'], target_date, session_type, litres, fat, snf, water, ph, temp, 
            pricing['base_rate'], pricing['deduction'], pricing['final_rate'], 
            pricing['amount'], pricing['deduction_reason']
        ))
        count += 1
        
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully generated {count} records for {session_type} on {target_date}")

if __name__ == "__main__":
    import sys
    sess = sys.argv[1] if len(sys.argv) > 1 else "FN"
    generate_session_data(sess)
