import os
import mysql.connector
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# Token Handling
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8651377308:AAGQlFuV67nGQe3MB23E0V4kiRtToyRMR6g")

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Mmani2005@")
DB_NAME = os.getenv("DB_NAME", "milk_collection")

# Temporary state for unlinked users selecting language
# chat_id -> 'en' or 'ta'
PENDING_LANGUAGES = {}

# Localization Dictionary
MESSAGES = {
    'en': {
        'welcome': "🥛 *Welcome to MilkHub AI*\n\nPlease choose your language to continue:",
        'lang_set': "✅ Language set to English\n\nPlease login using your mobile number.\n\nExample:\n`/login 7708400605`",
        'need_start': "❌ Please choose language first using /start",
        'login_req': "❌ Please provide your mobile number. Example: `/login 9876543210`",
        'login_success': "✅ Login successful.\nWelcome {name}\n\nUse /help to view commands.",
        'login_fail': "❌ Mobile number not registered.",
        'already_logged': "✅ You are already logged in as {name}.",
        'logged_out': "✅ Logged out successfully.",
        'no_reports': "No entries found.",
        'today_header': "🗓️ *Today's Entries*\n\n",
        'summary_header': "📈 *Current Month Summary*\n\n",
        'quality_header': "🔬 *Latest Quality Report*\n\n",
        'ai_header': "--- MilkHub AI Insights ---\n\n",
        'help_text': (
            "🛠️ *Available Commands*\n\n"
            "/today - Today's entries\n"
            "/summary - Monthly summary\n"
            "/report - Last 5 entries\n"
            "/quality - Milk quality grade\n"
            "/rate - Current milk rate\n"
            "/ai - Smart AI insights\n"
            "/language - Change language\n"
            "/logout - Unlink account\n"
            "/help - Show this message"
        )
    },
    'ta': {
        'welcome': "🥛 *MilkHub AI-க்கு வரவேற்கிறோம்*\n\nதொடர உங்கள் மொழியைத் தேர்ந்தெடுக்கவும்:",
        'lang_set': "✅ மொழி தமிழ் ஆக அமைக்கப்பட்டது\n\nதங்களின் மொபைல் எண்ணை பயன்படுத்தி உள்நுழைக.\n\nஉதாரணம்:\n`/login 7708400605`",
        'need_start': "❌ முதலில் /start பயன்படுத்தி மொழியைத் தேர்ந்தெடுக்கவும்",
        'login_req': "❌ உங்கள் மொபைல் எண்ணை வழங்கவும். உதாரணம்: `/login 9876543210`",
        'login_success': "✅ உள்நுழைவு வெற்றி.\nவரவேற்கிறோம் {name}\n\nகட்டளைகளைப் பார்க்க /help பயன்படுத்தவும்.",
        'login_fail': "❌ மொபைல் எண் பதிவு செய்யப்படவில்லை.",
        'already_logged': "✅ நீங்கள் ஏற்கனவே {name} ஆக உள்நுழைந்துள்ளீர்கள்.",
        'logged_out': "✅ வெற்றிகரமாக வெளியேறினீர்கள்.",
        'no_reports': "பதிவுகள் எதுவும் இல்லை.",
        'today_header': "🗓️ *இன்றைய பதிவுகள்*\n\n",
        'summary_header': "📈 *நடப்பு மாத சுருக்கம்*\n\n",
        'quality_header': "🔬 *சமீபத்திய தர அறிக்கை*\n\n",
        'ai_header': "--- MilkHub AI நுண்ணறிவு ---\n\n",
        'help_text': (
            "🛠️ *கிடைக்கக்கூடிய கட்டளைகள்*\n\n"
            "/today - இன்றைய பதிவுகள்\n"
            "/summary - மாதாந்திர சுருக்கம்\n"
            "/report - கடைசி 5 பதிவுகள்\n"
            "/quality - பால் தரம்\n"
            "/rate - இன்றைய விலை நிலவரம்\n"
            "/ai - AI ஆலோசனைகள்\n"
            "/language - மொழியை மாற்ற\n"
            "/logout - கணக்கை நீக்க\n"
            "/help - வழிமுறைகள்"
        )
    }
}

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME
    )

def get_user(chat_id):
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("SELECT * FROM users WHERE telegram_chat_id = %s", (str(chat_id),))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return user

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = get_user(chat_id)
    
    if user:
        lang = user.get('language', 'en')
        await update.message.reply_text(MESSAGES[lang]['help_text'], parse_mode="Markdown")
        return

    # If not linked, show language choice
    keyboard = [
        [
            InlineKeyboardButton("🇮🇳 தமிழ்", callback_data='lang_ta'),
            InlineKeyboardButton("🇬🇧 English", callback_data='lang_en')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(MESSAGES['en']['welcome'], reply_markup=reply_markup, parse_mode="Markdown")

async def language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat_id
    await query.answer()

    lang_choice = 'ta' if query.data == 'lang_ta' else 'en'
    PENDING_LANGUAGES[chat_id] = lang_choice

    # Update language in DB if user is already linked
    # (Though usually people click this before login)
    user = get_user(chat_id)
    if user:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET language = %s WHERE telegram_chat_id = %s", (lang_choice, str(chat_id)))
        conn.commit()
        cursor.close()
        conn.close()

    await query.edit_message_text(MESSAGES[lang_choice]['lang_set'], parse_mode="Markdown")

async def login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    
    # 1. Check if user already linked
    existing_user = get_user(chat_id)
    if existing_user:
        lang = existing_user.get('language', 'en')
        await update.message.reply_text(MESSAGES[lang]['already_logged'].format(name=existing_user['name']))
        return

    # 2. Check if language chosen
    lang = PENDING_LANGUAGES.get(chat_id)
    if not lang:
        await update.message.reply_text(MESSAGES['en']['need_start'] + "\n" + MESSAGES['ta']['need_start'])
        return

    # 3. Handle login input
    if not args:
        await update.message.reply_text(MESSAGES[lang]['login_req'])
        return

    phone = args[0]
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    
    # Check if phone exists
    cursor.execute("SELECT id, name FROM users WHERE phone = %s AND role = 'farmer'", (phone,))
    farmer = cursor.fetchone()
    
    if farmer:
        # 4. Unlink anyone else using this chat ID (Persistence Fix)
        cursor.execute("UPDATE users SET telegram_chat_id = NULL WHERE telegram_chat_id = %s", (str(chat_id),))
        
        # 5. Link this farmer
        cursor.execute(
            "UPDATE users SET telegram_chat_id = %s, language = %s WHERE id = %s",
            (str(chat_id), lang, farmer['id'])
        )
        conn.commit()
        
        # Clear pending language
        if chat_id in PENDING_LANGUAGES:
            del PENDING_LANGUAGES[chat_id]
            
        await update.message.reply_text(MESSAGES[lang]['login_success'].format(name=farmer['name']), parse_mode="Markdown")
    else:
        await update.message.reply_text(MESSAGES[lang]['login_fail'])

    cursor.close()
    conn.close()

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = get_user(chat_id)
    if not user:
        await update.message.reply_text("❌ Not logged in.")
        return

    lang = user.get('language', 'en')
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True)
    cursor.execute("UPDATE users SET telegram_chat_id = NULL WHERE telegram_chat_id = %s", (str(chat_id),))
    conn.commit()
    cursor.close()
    conn.close()
    
    await update.message.reply_text(MESSAGES[lang]['logged_out'])

async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("🔒 /start")
        return
    
    lang = user.get('language', 'en')
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("SELECT * FROM milk_records WHERE farmer_id = %s AND date = CURDATE() ORDER BY id DESC", (user['id'],))
    records = cursor.fetchall()
    cursor.close()
    conn.close()

    if not records:
        await update.message.reply_text(MESSAGES[lang]['no_reports'])
        return

    resp = MESSAGES[lang]['today_header']
    for r in records:
        resp += f"📍 {r['date']} ({r['session']})\n"
        resp += f"🥛 {r['litres']} L | {r['fat']}% Fat\n"
        resp += f"💰 ₹{r['amount']:.0f}\n\n"
    
    await update.message.reply_text(resp, parse_mode="Markdown")

async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_chat.id)
    if not user: return
    
    lang = user.get('language', 'en')
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("""
        SELECT SUM(litres) as total_litres, SUM(amount) as total_amount, AVG(fat) as avg_fat 
        FROM milk_records WHERE farmer_id = %s AND MONTH(date) = MONTH(CURDATE()) AND YEAR(date) = YEAR(CURDATE())
    """, (user['id'],))
    s = cursor.fetchone()
    cursor.close()
    conn.close()

    if not s or not s['total_litres']:
        await update.message.reply_text(MESSAGES[lang]['no_reports'])
        return

    resp = MESSAGES[lang]['summary_header']
    resp += f"📦 Total: {s['total_litres']:.1f} L\n"
    resp += f"📊 Avg Fat: {s['avg_fat']:.2f}%\n"
    resp += f"💵 Amount: ₹{s['total_amount']:.0f}"
    
    await update.message.reply_text(resp, parse_mode="Markdown")

async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_chat.id)
    if not user: return
    
    lang = user.get('language', 'en')
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("""
        SELECT * FROM milk_records 
        WHERE farmer_id = %s 
        ORDER BY date DESC, id DESC LIMIT 5
    """, (user['id'],))
    records = cursor.fetchall()
    cursor.close()
    conn.close()

    if not records:
        await update.message.reply_text(MESSAGES[lang]['no_reports'])
        return

    resp = f"📜 *Last 5 Entries*\n\n"
    for r in records:
        resp += f"📍 {r['date']} ({r['session']})\n"
        resp += f"🥛 {r['litres']} L | {r['fat']}% Fat\n"
        resp += f"💰 ₹{r['amount']:.0f}\n\n"
    
    await update.message.reply_text(resp, parse_mode="Markdown")

async def rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_chat.id)
    if not user: return
    
    lang = user.get('language', 'en')
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("SELECT base_rate FROM milk_rates WHERE date = CURDATE()")
    r = cursor.fetchone()
    cursor.close()
    conn.close()

    if r:
        msg = f"💹 *Today's Milk Rate*\n\nPrice per Litre: ₹{r['base_rate']:.2f}"
    else:
        msg = "💹 Rate for today is not yet updated by Admin."
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def quality(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_chat.id)
    if not user: return
    
    lang = user.get('language', 'en')
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("""
        SELECT fat, water_percent FROM milk_records 
        WHERE farmer_id = %s ORDER BY date DESC, id DESC LIMIT 1
    """, (user['id'],))
    r = cursor.fetchone()
    cursor.close()
    conn.close()

    if not r:
        await update.message.reply_text(MESSAGES[lang]['no_reports'])
        return

    # Simple local quality check
    f, w = float(r['fat']), float(r['water_percent'] or 0)
    q = "Excellent" if f >= 4.0 and w <= 5.0 else ("Good" if f >= 3.5 and w <= 10.0 else "Average")
    
    resp = MESSAGES[lang]['quality_header']
    resp += f"🥛 Last Fat: {f}%\n"
    resp += f"💧 Water: {w}%\n"
    resp += f"⭐ Status: *{q}*"
    
    await update.message.reply_text(resp, parse_mode="Markdown")

async def ai_insights(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_chat.id)
    if not user: return
    
    lang = user.get('language', 'en')
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("""
        SELECT recommendation, confidence_score FROM ml_predictions 
        WHERE farmer_id = %s ORDER BY date DESC LIMIT 1
    """, (user['id'],))
    ai = cursor.fetchone()
    cursor.close()
    conn.close()

    if not ai:
        await update.message.reply_text("🤖 AI is still analyzing your patterns. Check back tomorrow!")
        return

    resp = MESSAGES[lang]['ai_header']
    resp += f"💡 {ai['recommendation']}\n"
    resp += f"🎯 Confidence: {ai['confidence_score']}"
    
    await update.message.reply_text(resp, parse_mode="Markdown")

# Main and other boilerplate updated for async and flow
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("login", login))
    app.add_handler(CommandHandler("logout", logout))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("report", report))
    app.add_handler(CommandHandler("rate", rate))
    app.add_handler(CommandHandler("quality", quality))
    app.add_handler(CommandHandler("ai", ai_insights))
    
    # NEW Admin Automation Commands
    from sample_data_generator import generate_session_data
    from scheduler import check_and_remind, daily_absent_summary

    async def admin_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = get_user(update.effective_chat.id)
        if not user or user['role'] != 'admin':
            await update.message.reply_text("⛔ Restricted to Admin only.")
            return
        
        sess = context.args[0].upper() if context.args else "FN"
        if sess not in ["FN", "AN"]:
            await update.message.reply_text("❓ Use: `/sync FN` or `/sync AN`", parse_mode="Markdown")
            return
            
        await update.message.reply_text(f"⏳ Syncing {sess} session data...")
        generate_session_data(sess)
        await update.message.reply_text(f"✅ Data generated for {sess}.")

    async def admin_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = get_user(update.effective_chat.id)
        if not user or user['role'] != 'admin': return
        
        sess = context.args[0].upper() if context.args else "FN"
        await update.message.reply_text(f"🔔 Sending reminders for {sess}...")
        check_and_remind(sess)
        await update.message.reply_text(f"✅ Reminders sent.")

    app.add_handler(CommandHandler("sync", admin_sync))
    app.add_handler(CommandHandler("remind", admin_remind))
    
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("language", start))
    
    app.add_handler(CallbackQueryHandler(language_callback))

    print("Milk Angel Bot is waking up...")
    app.run_polling()

if __name__ == "__main__":
    main()
