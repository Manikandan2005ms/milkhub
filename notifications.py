import requests
import os

TELEGRAM_BOT_TOKEN = "8651377308:AAGQlFuV67nGQe3MB23E0V4kiRtToyRMR6g"

# Localization Dictionary
MESSAGES = {
    'en': {
        'instant_added': "🥛 *Milk Entry Added Successfully*\n\nDate: {date}\nSession: {session}\nLitres: {litres} L\nFat: {fat}%\nSNF: {snf}\nWater: {water_percent}%\nRate: ₹{rate}/L\nAmount: ₹{amount}\nQuality: {quality}\n\nThank you.",
        'daily_summary': "📊 *Today's Milk Summary*\n\nDate: {date}\n\nMorning: {morning} L\nEvening: {evening} L\n\nTotal Litres: {total_litres} L\nAverage Fat: {avg_fat}%\nTotal Amount: ₹{total_amount}\n\nGreat job! 👍",
        'monthly_summary': "📅 *Monthly Milk Report*\n\nMonth: {month}\n\nTotal Litres: {total_litres} L\nAverage Fat: {avg_fat}%\nTotal Amount: ₹{total_amount}\nEntries: {entry_count}",
        'no_entry': "No Entry"
    },
    'ta': {
        'instant_added': "🥛 *இன்று பால் பதிவு செய்யப்பட்டது*\n\nதேதி: {date}\nநேரம்: {session}\nஅளவு: {litres} L\nகொழுப்பு: {fat}%\nSNF: {snf}\nதண்ணீர்: {water_percent}%\nவிலை: ₹{rate}/L\nமொத்தம்: ₹{amount}\nதரம்: {quality}\n\nநன்றி.",
        'daily_summary': "📊 *இன்றைய பால் அறிக்கை*\n\nதேதி: {date}\n\nகாலை: {morning} L\nமாலை: {evening} L\n\nமொத்த அளவு: {total_litres} L\nசராசரி கொழுப்பு: {avg_fat}%\nமொத்த தொகை: ₹{total_amount}\n\nவாழ்த்துகள்! 👍",
        'monthly_summary': "📅 *மாதாந்திர பால் அறிக்கை*\n\nமாதம்: {month}\n\nமொத்த அளவு: {total_litres} L\nசராசரி கொழுப்பு: {avg_fat}%\nமொத்த தொகை: ₹{total_amount}\nபதிவுகள்: {entry_count}",
        'no_entry': "பதிவு இல்லை"
    }
}

def send_telegram_msg(chat_id, text):
    if not chat_id: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Telegram notification failed: {e}")

def notify_instant_entry(chat_id, lang, data):
    lang = lang if lang in MESSAGES else 'en'
    # Quality logic (Aligned with new ML model)
    quality = "Medium"
    if float(data['fat']) >= 4.0 and float(data.get('water_percent', 0)) <= 3.0:
        quality = "High"
    elif float(data['fat']) < 3.0 or float(data.get('water_percent', 0)) > 10:
        quality = "Low"
    
    sess_map = {'FN': 'Morning' if lang == 'en' else 'காலை', 'AN': 'Evening' if lang == 'en' else 'மாலை'}
    
    text = MESSAGES[lang]['instant_added'].format(
        date=data['date'],
        session=sess_map.get(data['session'], data['session']),
        litres=data['litres'],
        fat=data['fat'],
        snf=data.get('snf', 'N/A'),
        water_percent=data.get('water_percent', 0),
        rate=data['rate'],
        amount=data['amount'],
        quality=quality
    )
    send_telegram_msg(chat_id, text)

def notify_daily_summary(chat_id, lang, data):
    lang = lang if lang in MESSAGES else 'en'
    text = MESSAGES[lang]['daily_summary'].format(
        date=data['date'],
        morning=data.get('morning', MESSAGES[lang]['no_entry']),
        evening=data.get('evening', MESSAGES[lang]['no_entry']),
        total_litres=data['total_litres'],
        avg_fat=data['avg_fat'],
        total_amount=data['total_amount']
    )
    send_telegram_msg(chat_id, text)

def notify_monthly_summary(chat_id, lang, data):
    lang = lang if lang in MESSAGES else 'en'
    text = MESSAGES[lang]['monthly_summary'].format(
        month=data['month'],
        total_litres=data['total_litres'],
        avg_fat=data['avg_fat'],
        total_amount=data['total_amount'],
        entry_count=data['entry_count']
    )
    send_telegram_msg(chat_id, text)
