import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
MONOBANK_TOKEN = os.getenv("MONOBANK_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

CATEGORIES = {
    "Продукты": ["rewe", "lidl", "aldi", "edeka", "penny", "netto", "kaufland", "billa", "spar", "metro", "продукты", "супермаркет", "market"],
    "Кафе": ["cafe", "coffee", "starbucks", "kaffee", "restaurant", "pizza", "burger", "mcdonalds", "kfc", "subway", "bar", "кафе", "ресторан", "доставка", "lieferando"],
    "Транспорт": ["bvg", "db ", "deutsche bahn", "ubahn", "s-bahn", "bus", "uber", "bolt", "taxi", "mvg", "проездной", "транспорт", "tankstelle", "aral", "shell"],
    "Здоровье": ["apotheke", "pharmacy", "arzt", "doctor", "медицин", "аптека", "psycholog", "линзы", "brillen", "здоровье", "dm ", "rossmann"],
    "Фитнесс": ["fitness", "gym", "sport", "yoga", "fitnessstudio", "фитнес", "спорт"],
    "Одежда": ["zara", "h&m", "hm ", "primark", "c&a", "uniqlo", "zalando", "одежда", "kleidung", "mode"],
    "Связь плюс подписки": ["netflix", "spotify", "amazon prime", "apple", "google", "телефон", "симка", "vodafone", "telekom", "o2", "congstar", "подписка", "abo"],
    "Немецкий": ["schule", "kurs", "deutsch", "немецкий", "volkshochschule", "vhs", "ahso"],
    "Подарки": ["подарок", "geschenk", "blumen", "цветы"],
    "Для дома": ["ikea", "obi", "bauhaus", "hornbach", "saturn", "mediamarkt", "дом", "haushalt"],
    "Уход": ["friseur", "kosmetik", "beauty", "nails", "массаж", "уход"],
}

MONTH_NAMES = {
    1: "янв.", 2: "февр.", 3: "март", 4: "апр.",
    5: "май", 6: "июн.", 7: "июл.", 8: "авг.",
    9: "сент.", 10: "окт.", 11: "нояб.", 12: "дек."
}


def get_category(description: str) -> str:
    desc = description.lower()
    for category, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw in desc:
                return category
    return "Прочее"


def get_google_sheet():
    creds_json = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def parse_date(date_val: str):
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(date_val)[:10], fmt)
        except ValueError:
            continue
    return None


def get_uah_to_eur_rate() -> float:
    try:
        response = requests.get("https://api.monobank.ua/bank/currency", timeout=5)
        for rate in response.json():
            if rate.get("currencyCodeA") == 978 and rate.get("currencyCodeB") == 980:
                return rate.get("rateSell", 0) or rate.get("rateCross", 45)
        return 45.0
    except Exception:
        return 45.0


def fetch_mono_transactions(days: int = 7) -> list:
    now = int(datetime.now().timestamp())
    from_time = int((datetime.now() - timedelta(days=days)).timestamp())
    url = f"https://api.monobank.ua/personal/statement/0/{from_time}/{now}"
    headers = {"X-Token": MONOBANK_TOKEN}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return []
    eur_rate = get_uah_to_eur_rate()
    transactions = []
    for t in response.json():
        if t["amount"] >= 0:
            continue
        amount_uah = abs(t["amount"]) / 100
        currency = t.get("currencyCode", 980)
        if currency == 978:
            amount_eur = abs(t.get("operationAmount", t["amount"])) / 100
        else:
            amount_eur = round(amount_uah / eur_rate, 2)
        desc = t.get("description", "")
        date = datetime.fromtimestamp(t["time"])
        transactions.append({"date": date, "description": desc, "amount": amount_eur, "category": get_category(desc), "source": "Monobank"})
    return transactions


def save_to_sheet(transactions: list) -> int:
    if not transactions:
        return 0
    sheet = get_google_sheet()
    try:
        ws = sheet.worksheet("Повседневные")
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet("Повседневные", rows=1000, cols=16)
        ws.append_row(["Дата", "Месяц", "Категория", "Тип", "Стоимость", "Комментарий", "Источник"])
    existing = ws.col_values(6)
    added = 0
    for t in transactions:
        date_str = t["date"].strftime("%d.%m.%Y")
        month = MONTH_NAMES[t["date"].month]
        comment = t["description"]
        marker = f"{date_str}|{comment}"
        if marker in existing:
            continue
        ws.append_row([date_str, month, t["category"], "Расход", round(t["amount"], 2), comment, t["source"]])
        existing.append(marker)
        added += 1
    return added


def parse_csv_transactions(text: str, source: str) -> list:
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    transactions = []
    header = lines[0].lower() if lines else ""
    for line in lines[1:]:
        parts = [p.strip().strip('"') for p in line.split(",")]
        try:
            if "revolut" in source.lower() or "started date" in header:
                date = datetime.strptime(parts[0][:10], "%Y-%m-%d")
                desc = parts[2]
                amount = float(parts[3])
                if amount >= 0:
                    continue
                amount = abs(amount)
            elif "paypal" in source.lower() or "transaction id" in header:
                date = datetime.strptime(parts[0], "%d/%m/%Y")
                desc = parts[3]
                amount = float(parts[7].replace(".", "").replace(",", "."))
                if amount >= 0:
                    continue
                amount = abs(amount)
            elif "sparkasse" in source.lower() or "buchungstag" in header:
                parts = line.split(";")
                date = datetime.strptime(parts[0], "%d.%m.%y")
                desc = parts[2] + " " + parts[3]
                amount = float(parts[4].replace(".", "").replace(",", "."))
                if amount >= 0:
                    continue
                amount = abs(amount)
            else:
                continue
            transactions.append({"date": date, "description": desc, "amount": amount, "category": get_category(desc), "source": source})
        except (ValueError, IndexError):
            continue
    return transactions


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 Привет! Я твой финансовый бот.\n\n"
        "📥 /sync — загрузить транзакции из Monobank за 7 дней\n"
        "📊 /summary — сводка расходов за текущий месяц\n"
        "🔍 /debug — показать сырые данные из таблицы\n"
        "📎 Отправь CSV — загружу из Revolut, PayPal или Sparkasse"
    )
    await update.message.reply_text(text)


async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sheet = get_google_sheet()
        ws = sheet.worksheet("Повседневные")
        data = ws.get_all_records()
        now = datetime.now()
        lines = [f"Всего строк: {len(data)}", f"Текущий месяц: {MONTH_NAMES[now.month]}", ""]
        april_rows = [r for r in data if "апр" in str(r.get("Месяц", "")) and str(r.get("Дата","")).strip()]
        expense_rows = [r for r in april_rows if r.get("Тип") == "Расход"]
        total_debug = sum(float(str(r.get("Стоимость",0)).replace(",",".")) for r in expense_rows)
        lines.append(f"Апрельских строк: {len(april_rows)}, расходов: {len(expense_rows)}")
        lines.append(f"Сумма: {total_debug:.2f} €")
        lines.append("")
        for r in expense_rows[:8]:
            lines.append(f"  {r.get('Дата','')} | {r.get('Категория','')} | {r.get('Стоимость','')}")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ {str(e)}")


async def sync_mono(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Загружаю транзакции из Monobank...")
    try:
        transactions = fetch_mono_transactions(days=7)
        if not transactions:
            await update.message.reply_text("✅ Новых транзакций нет.")
            return
        added = save_to_sheet(transactions)
        by_cat = {}
        for t in transactions:
            by_cat[t["category"]] = by_cat.get(t["category"], 0) + t["amount"]
        lines = [f"✅ Загружено {added} новых из {len(transactions)} транзакций!\n", "📊 По категориям:"]
        for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
            lines.append(f"  • {cat}: {amt:.2f} €")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Считаю расходы за месяц...")
    try:
        sheet = get_google_sheet()
        ws = sheet.worksheet("Повседневные")
        data = ws.get_all_records()
        now = datetime.now()
        current_month_name = MONTH_NAMES[now.month]
        by_cat = {}
        total = 0
        for r in data:
            if r.get("Тип") != "Расход":
                continue
            date_val = str(r.get("Дата", "")).strip()
            if not date_val:
                continue
            row_date = parse_date(date_val)
            if not row_date:
                continue
            if row_date.month != now.month or row_date.year != now.year:
                continue
            cat = r.get("Категория", "Прочее") or "Прочее"
            try:
                amt_raw = r.get("Стоимость", 0)
                # Google Sheets с русской локалью убирает запятую: 15,49 → 1549, 563,00 → 56300
                # Поэтому всегда делим на 100
                amt = float(str(amt_raw).replace(",", ".").replace(" ", "")) / 100
            except ValueError:
                continue
            by_cat[cat] = by_cat.get(cat, 0) + amt
            total += amt
        if not by_cat:
            await update.message.reply_text(f"📊 За {current_month_name} {now.year} расходов не найдено.")
            return
        lines = [f"📊 Расходы за {current_month_name} {now.year}:\n"]
        for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
            pct = amt / total * 100 if total else 0
            lines.append(f"  • {cat}: {amt:.2f} € ({pct:.0f}%)")
        lines.append(f"\n💰 Итого: {total:.2f} €")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc.file_name.endswith(".csv"):
        await update.message.reply_text("Пожалуйста, отправь CSV файл.")
        return
    fname = doc.file_name.lower()
    if "revolut" in fname:
        source = "Revolut"
    elif "paypal" in fname:
        source = "PayPal"
    elif "sparkasse" in fname or "export" in fname:
        source = "Sparkasse"
    else:
        keyboard = [
            [InlineKeyboardButton("Revolut", callback_data=f"csv_revolut_{doc.file_id}")],
            [InlineKeyboardButton("PayPal", callback_data=f"csv_paypal_{doc.file_id}")],
            [InlineKeyboardButton("Sparkasse", callback_data=f"csv_sparkasse_{doc.file_id}")],
        ]
        await update.message.reply_text("Из какого банка этот файл?", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    await process_csv(update, context, doc.file_id, source)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_", 2)
    if parts[0] == "csv" and len(parts) == 3:
        source_map = {"revolut": "Revolut", "paypal": "PayPal", "sparkasse": "Sparkasse"}
        source = source_map.get(parts[1], parts[1])
        await process_csv(query, context, parts[2], source)


async def process_csv(update_or_query, context, file_id: str, source: str):
    reply = update_or_query.message.reply_text if hasattr(update_or_query, 'message') else update_or_query.edit_message_text
    await reply(f"⏳ Обрабатываю файл из {source}...")
    try:
        file = await context.bot.get_file(file_id)
        content = bytes(await file.download_as_bytearray()).decode("utf-8", errors="ignore")
        transactions = parse_csv_transactions(content, source)
        if not transactions:
            await reply("❌ Не удалось распознать транзакции. Проверь формат файла.")
            return
        added = save_to_sheet(transactions)
        by_cat = {}
        for t in transactions:
            by_cat[t["category"]] = by_cat.get(t["category"], 0) + t["amount"]
        lines = [f"✅ Из {source}: загружено {added} новых из {len(transactions)} транзакций!\n", "📊 По категориям:"]
        for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
            lines.append(f"  • {cat}: {amt:.2f} €")
        await reply("\n".join(lines))
    except Exception as e:
        await reply(f"❌ Ошибка: {str(e)}")


async def weekly_sync(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    try:
        transactions = fetch_mono_transactions(days=7)
        added = save_to_sheet(transactions)
        by_cat = {}
        total = 0
        for t in transactions:
            by_cat[t["category"]] = by_cat.get(t["category"], 0) + t["amount"]
            total += t["amount"]
        lines = ["📅 Еженедельная сводка Monobank\n", f"Загружено {added} новых транзакций\n", "📊 По категориям:"]
        for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
            lines.append(f"  • {cat}: {amt:.2f} €")
        lines.append(f"\n💰 Итого за неделю: {total:.2f} €")
        lines.append("\n📎 Не забудь загрузить CSV из Revolut, PayPal и Sparkasse!")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Ошибка: {str(e)}")


async def setup_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    context.job_queue.run_repeating(weekly_sync, interval=timedelta(weeks=1), first=timedelta(seconds=5), chat_id=chat_id, name=f"weekly_{chat_id}")
    await update.message.reply_text("✅ Еженедельная синхронизация настроена! Каждое воскресенье в 20:00 📊")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("sync", sync_mono))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("weekly", setup_weekly))
    app.add_handler(CommandHandler("debug", debug))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(handle_callback))
    print("Бот запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
