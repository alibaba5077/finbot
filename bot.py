import os
import io
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
    "Продукты": ["rewe", "lidl", "aldi", "edeka", "penny", "netto", "kaufland", "billa", "spar", "metro", "продукты", "market"],
    "Кафе": ["cafe", "coffee", "starbucks", "kaffee", "restaurant", "pizza", "burger", "mcdonalds", "kfc", "subway", "bar", "кафе", "ресторан", "lieferando"],
    "Транспорт": ["bvg", "db ", "deutsche bahn", "ubahn", "s-bahn", "uber", "bolt", "taxi", "mvg", "проездной", "транспорт", "tankstelle", "aral", "shell"],
    "Здоровье": ["apotheke", "pharmacy", "arzt", "doctor", "аптека", "psycholog", "линзы", "brillen", "rossmann", "petrishcheva", "петришева", "marina"],
    "Фитнесс": ["fitness", "gym", "sport", "yoga", "fitnessstudio"],
    "Одежда": ["zara", "h&m", "primark", "c&a", "uniqlo", "zalando", "одежда"],
    "Связь плюс подписки": ["netflix", "spotify", "amazon prime", "apple", "google", "симка", "vodafone", "telekom", "o2", "congstar", "abo"],
    "Немецкий": ["schule", "kurs", "deutsch", "немецкий", "volkshochschule", "vhs", "ahso", "netz.schule", "ксенія", "ксения"],
    "Подарки": ["подарок", "geschenk", "blumen", "цветы"],
    "Для дома": ["ikea", "obi", "bauhaus", "hornbach", "saturn", "mediamarkt", "haushalt"],
    "Уход": ["friseur", "kosmetik", "beauty", "nails", "массаж"],
}

MONTH_NAMES = {
    1: "янв.", 2: "февр.", 3: "март", 4: "апр.",
    5: "май", 6: "июн.", 7: "июл.", 8: "авг.",
    9: "сент.", 10: "окт.", 11: "нояб.", 12: "дек."
}

CAT_MAP = {
    "продукты": "Продукты", "еда": "Продукты",
    "кафе": "Кафе", "ресторан": "Кафе", "кофе": "Кафе",
    "транспорт": "Транспорт", "метро": "Транспорт", "бвг": "Транспорт",
    "здоровье": "Здоровье", "врач": "Здоровье", "аптека": "Здоровье", "психолог": "Здоровье",
    "фитнесс": "Фитнесс", "спорт": "Фитнесс", "фитнес": "Фитнесс",
    "одежда": "Одежда",
    "немецкий": "Немецкий", "курсы": "Немецкий",
    "подарки": "Подарки", "подарок": "Подарки",
    "дом": "Для дома",
    "уход": "Уход",
    "прочее": "Прочее",
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
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(str(date_val)[:10], fmt)
        except ValueError:
            continue
    return None


def parse_amount(amt_raw) -> float:
    if isinstance(amt_raw, (float, int)):
        return float(amt_raw)
    s = str(amt_raw).strip().replace(" ", "")
    if "," in s:
        s = s.replace(",", ".")
    return float(s)


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
        currency = t.get("currencyCode", 980)
        if currency == 978:
            amount_eur = abs(t.get("operationAmount", t["amount"])) / 100
        else:
            amount_eur = round(abs(t["amount"]) / 100 / eur_rate, 2)
        desc = t.get("description", "")
        date = datetime.fromtimestamp(t["time"])
        transactions.append({"date": date, "description": desc, "amount": amount_eur, "category": get_category(desc), "source": "Monobank", "type": "Расход"})
    return transactions


def save_to_sheet(transactions: list) -> int:
    if not transactions:
        return 0
    sheet = get_google_sheet()
    try:
        ws = sheet.worksheet("Повседневные")
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet("Повседневные", rows=1000, cols=7)
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
        ws.append_row([date_str, month, t.get("category", ""), t.get("type", "Расход"), round(t["amount"], 2), comment, t.get("source", "")])
        existing.append(marker)
        added += 1
    return added


def get_month_data(month: int, year: int):
    sheet = get_google_sheet()
    ws = sheet.worksheet("Повседневные")
    data = ws.get_all_records()
    expenses = {}
    income = {}
    for r in data:
        date_val = str(r.get("Дата", "")).strip()
        if not date_val:
            continue
        row_date = parse_date(date_val)
        if not row_date or row_date.month != month or row_date.year != year:
            continue
        try:
            amt = parse_amount(r.get("Стоимость", 0))
        except ValueError:
            continue
        if r.get("Тип") == "Расход":
            cat = r.get("Категория", "Прочее") or "Прочее"
            expenses[cat] = expenses.get(cat, 0) + amt
        elif r.get("Тип") == "Доход":
            cat = r.get("Категория", "") or "Доход"
            income[cat] = income.get(cat, 0) + amt
    return expenses, income


def parse_csv_revolut(text: str) -> list:
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    transactions = []
    for line in lines[1:]:
        parts = [p.strip().strip('"') for p in line.split(",")]
        try:
            date = datetime.strptime(parts[0][:10], "%Y-%m-%d")
            desc = parts[2] if len(parts) > 2 else ""
            amount = float(parts[3]) if len(parts) > 3 else 0
            if amount >= 0:
                continue
            transactions.append({"date": date, "description": desc, "amount": abs(amount), "category": get_category(desc), "source": "Revolut", "type": "Расход"})
        except (ValueError, IndexError):
            continue
    return transactions


def parse_csv_paypal(text: str) -> list:
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    transactions = []
    for line in lines[1:]:
        parts = [p.strip().strip('"') for p in line.split(",")]
        try:
            date = datetime.strptime(parts[0], "%d/%m/%Y")
            desc = parts[3] if len(parts) > 3 else ""
            amt_str = parts[7] if len(parts) > 7 else "0"
            amount = float(amt_str.replace(".", "").replace(",", "."))
            if amount >= 0:
                continue
            transactions.append({"date": date, "description": desc, "amount": abs(amount), "category": get_category(desc), "source": "PayPal", "type": "Расход"})
        except (ValueError, IndexError):
            continue
    return transactions


def parse_csv_sparkasse(text: str) -> list:
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    transactions = []
    for line in lines[1:]:
        parts = line.split(";")
        try:
            date = parse_date(parts[0].strip())
            if not date:
                continue
            desc = " ".join(p.strip().strip('"') for p in parts[1:-1] if p.strip())
            amt_str = parts[-1].strip().strip('"').replace(" ", "")
            amount = float(amt_str.replace(".", "").replace(",", "."))
            tip = "Расход" if amount < 0 else "Доход"
            transactions.append({"date": date, "description": desc, "amount": abs(amount), "category": get_category(desc) if tip == "Расход" else "", "source": "Sparkasse", "type": tip})
        except (ValueError, IndexError):
            continue
    return transactions


def parse_pdf_sparkasse(pdf_bytes: bytes) -> list:
    try:
        import pdfplumber
        transactions = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if not row or len(row) < 3:
                            continue
                        date_val = str(row[0]).strip() if row[0] else ""
                        desc = str(row[1]).strip() if len(row) > 1 and row[1] else ""
                        amt_str = str(row[-1]).strip() if row[-1] else ""
                        date = parse_date(date_val)
                        if not date or not amt_str:
                            continue
                        try:
                            amount = float(amt_str.replace(".", "").replace(",", ".").replace(" ", "").replace("\u20ac", "").replace("+", ""))
                        except ValueError:
                            continue
                        tip = "Расход" if amount < 0 else "Доход"
                        transactions.append({"date": date, "description": desc, "amount": abs(amount), "category": get_category(desc) if tip == "Расход" else "", "source": "Sparkasse PDF", "type": tip})
        return transactions
    except Exception:
        return []


# ─── Handlers ────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я твой финансовый бот.\n\n"
        "📥 /sync — загрузить из Monobank за 7 дней\n"
        "📊 /summary — расходы и доходы за текущий месяц\n"
        "📅 /month 04.2026 — сводка за любой месяц\n"
        "📈 /year 2026 — итоги за весь год\n"
        "➕ /add 25.5 продукты lidl — добавить трату\n"
        "💚 /income 563 выплата — добавить доход\n"
        "🔔 /weekly — еженедельная синхронизация\n"
        "🗓 /monthly — ежемесячный отчёт 1-го числа\n"
        "📎 Отправь CSV или PDF — загружу из Revolut, PayPal или Sparkasse\n"
        "🔍 /debug — диагностика"
    )


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


async def show_month_summary(update, month: int, year: int):
    month_name = MONTH_NAMES[month]
    await update.message.reply_text(f"⏳ Считаю за {month_name} {year}...")
    try:
        expenses, income = get_month_data(month, year)
        total_exp = sum(expenses.values())
        total_inc = sum(income.values())
        if not expenses and not income:
            await update.message.reply_text(f"📊 За {month_name} {year} данных не найдено.")
            return
        lines = [f"📊 {month_name} {year}\n"]
        if expenses:
            lines.append("🔴 РАСХОДЫ:")
            for cat, amt in sorted(expenses.items(), key=lambda x: -x[1]):
                pct = amt / total_exp * 100 if total_exp else 0
                lines.append(f"  • {cat}: {amt:.2f} € ({pct:.0f}%)")
            lines.append(f"  Итого: {total_exp:.2f} €")
        if income:
            lines.append("\n🟢 ДОХОДЫ:")
            for cat, amt in sorted(income.items(), key=lambda x: -x[1]):
                lines.append(f"  • {cat}: {amt:.2f} €")
            lines.append(f"  Итого: {total_inc:.2f} €")
        balance = total_inc - total_exp
        sign = "+" if balance >= 0 else ""
        lines.append(f"\n💰 Баланс: {sign}{balance:.2f} €")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now()
    await show_month_summary(update, now.month, now.year)


async def month_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Укажи месяц: /month 04.2026")
        return
    try:
        parts = args[0].split(".")
        month, year = int(parts[0]), int(parts[1])
        await show_month_summary(update, month, year)
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Неверный формат. Пример: /month 04.2026")


async def year_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    year = int(args[0]) if args else datetime.now().year
    await update.message.reply_text(f"⏳ Считаю итоги за {year}...")
    try:
        sheet = get_google_sheet()
        ws = sheet.worksheet("Повседневные")
        data = ws.get_all_records()
        monthly = {}
        total_exp = 0
        total_inc = 0
        for r in data:
            date_val = str(r.get("Дата", "")).strip()
            if not date_val:
                continue
            row_date = parse_date(date_val)
            if not row_date or row_date.year != year:
                continue
            try:
                amt = parse_amount(r.get("Стоимость", 0))
            except ValueError:
                continue
            m = row_date.month
            if m not in monthly:
                monthly[m] = {"exp": 0, "inc": 0}
            if r.get("Тип") == "Расход":
                monthly[m]["exp"] += amt
                total_exp += amt
            elif r.get("Тип") == "Доход":
                monthly[m]["inc"] += amt
                total_inc += amt
        if not monthly:
            await update.message.reply_text(f"Данных за {year} не найдено.")
            return
        lines = [f"📈 Итоги {year}\n"]
        for m in sorted(monthly.keys()):
            exp = monthly[m]["exp"]
            inc = monthly[m]["inc"]
            bal = inc - exp
            sign = "+" if bal >= 0 else ""
            lines.append(f"{MONTH_NAMES[m]:6} | 🔴{exp:.0f}€ | 🟢{inc:.0f}€ | {sign}{bal:.0f}€")
        lines.append("\n" + "─" * 32)
        lines.append(f"Итого  | 🔴{total_exp:.2f}€ | 🟢{total_inc:.2f}€")
        bal = total_inc - total_exp
        lines.append(f"Баланс | 💰{'+' if bal >= 0 else ''}{bal:.2f}€")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Формат: /add сумма категория описание\n"
            "Пример: /add 25.5 продукты lidl\n"
            "Пример: /add 47 здоровье психолог\n\n"
            "Категории: продукты, кафе, транспорт, здоровье, фитнесс, одежда, немецкий, подарки, дом, уход, прочее"
        )
        return
    try:
        amount = float(args[0].replace(",", "."))
        cat_input = args[1].lower()
        description = " ".join(args[2:]) if len(args) > 2 else args[1]
        category = CAT_MAP.get(cat_input, get_category(cat_input))
        now = datetime.now()
        save_to_sheet([{"date": now, "description": description, "amount": amount, "category": category, "source": "Бот", "type": "Расход"}])
        await update.message.reply_text(
            f"✅ Добавлено!\n"
            f"  📁 {category}\n"
            f"  💶 {amount:.2f} €\n"
            f"  📝 {description}\n"
            f"  📅 {now.strftime('%d.%m.%Y')}"
        )
    except ValueError:
        await update.message.reply_text("❌ Неверная сумма. Пример: /add 25.5 продукты lidl")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def income_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Формат: /income сумма описание\nПример: /income 563 выплата")
        return
    try:
        amount = float(args[0].replace(",", "."))
        description = " ".join(args[1:])
        now = datetime.now()
        save_to_sheet([{"date": now, "description": description, "amount": amount, "category": "", "source": "Бот", "type": "Доход"}])
        await update.message.reply_text(
            f"✅ Доход добавлен!\n"
            f"  🟢 {amount:.2f} €\n"
            f"  📝 {description}\n"
            f"  📅 {now.strftime('%d.%m.%Y')}"
        )
    except ValueError:
        await update.message.reply_text("❌ Неверная сумма. Пример: /income 563 выплата")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sheet = get_google_sheet()
        ws = sheet.worksheet("Повседневные")
        data = ws.get_all_records()
        now = datetime.now()
        rows = [r for r in data if str(r.get("Дата", "")).strip() and parse_date(str(r.get("Дата", ""))) and parse_date(str(r.get("Дата", ""))).month == now.month]
        lines = [f"Всего строк: {len(data)}", f"Строк за текущий месяц: {len(rows)}", ""]
        for r in rows[:5]:
            amt_raw = r.get("Стоимость", 0)
            lines.append(f"  {r.get('Дата','')} | {r.get('Категория','')} | {r.get('Тип','')} | {amt_raw}({type(amt_raw).__name__})")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ {str(e)}")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    fname = doc.file_name.lower()
    is_pdf = fname.endswith(".pdf")
    is_csv = fname.endswith(".csv")
    if not is_pdf and not is_csv:
        await update.message.reply_text("Пожалуйста, отправь CSV или PDF файл.")
        return
    if "revolut" in fname:
        source = "Revolut"
    elif "paypal" in fname or "csr" in fname:
        source = "PayPal"
    elif "sparkasse" in fname or "umsatz" in fname or "export" in fname:
        source = "Sparkasse"
    else:
        keyboard = [
            [InlineKeyboardButton("Revolut", callback_data=f"file_revolut_{doc.file_id}")],
            [InlineKeyboardButton("PayPal", callback_data=f"file_paypal_{doc.file_id}")],
            [InlineKeyboardButton("Sparkasse", callback_data=f"file_sparkasse_{doc.file_id}")],
        ]
        await update.message.reply_text("Из какого банка этот файл?", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    await process_file(update, context, doc.file_id, source, is_pdf)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_", 2)
    if parts[0] == "file" and len(parts) == 3:
        source_map = {"revolut": "Revolut", "paypal": "PayPal", "sparkasse": "Sparkasse"}
        source = source_map.get(parts[1], parts[1])
        await process_file(query, context, parts[2], source, False)


async def process_file(update_or_query, context, file_id: str, source: str, is_pdf: bool):
    reply = update_or_query.message.reply_text if hasattr(update_or_query, "message") else update_or_query.edit_message_text
    await reply(f"⏳ Обрабатываю файл из {source}...")
    try:
        file = await context.bot.get_file(file_id)
        raw = bytes(await file.download_as_bytearray())
        if is_pdf:
            transactions = parse_pdf_sparkasse(raw)
        else:
            content = raw.decode("utf-8", errors="ignore")
            if source == "Revolut":
                transactions = parse_csv_revolut(content)
            elif source == "PayPal":
                transactions = parse_csv_paypal(content)
            else:
                transactions = parse_csv_sparkasse(content)
        if not transactions:
            await reply("❌ Не удалось распознать транзакции. Проверь формат файла.")
            return
        added = save_to_sheet(transactions)
        by_cat = {}
        for t in transactions:
            if t.get("type") == "Расход":
                by_cat[t["category"]] = by_cat.get(t["category"], 0) + t["amount"]
        lines = [f"✅ Из {source}: загружено {added} новых из {len(transactions)} транзакций!\n", "📊 Расходы по категориям:"]
        for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
            lines.append(f"  • {cat}: {amt:.2f} €")
        await reply("\n".join(lines))
    except Exception as e:
        await reply(f"❌ Ошибка: {str(e)}")


async def send_monthly_report(context, chat_id: int):
    now = datetime.now()
    month = now.month - 1 if now.month > 1 else 12
    year = now.year if now.month > 1 else now.year - 1
    try:
        expenses, income = get_month_data(month, year)
        total_exp = sum(expenses.values())
        total_inc = sum(income.values())
        balance = total_inc - total_exp
        month_name = MONTH_NAMES[month]
        lines = [f"🗓 Ежемесячный отчёт — {month_name} {year}\n"]
        if expenses:
            lines.append("🔴 Расходы:")
            for cat, amt in sorted(expenses.items(), key=lambda x: -x[1]):
                pct = amt / total_exp * 100 if total_exp else 0
                lines.append(f"  • {cat}: {amt:.2f}€ ({pct:.0f}%)")
            lines.append(f"  Итого: {total_exp:.2f}€\n")
        if income:
            lines.append("🟢 Доходы:")
            for cat, amt in sorted(income.items(), key=lambda x: -x[1]):
                lines.append(f"  • {cat}: {amt:.2f}€")
            lines.append(f"  Итого: {total_inc:.2f}€\n")
        sign = "+" if balance >= 0 else ""
        lines.append(f"💰 Баланс: {sign}{balance:.2f}€")
        if expenses:
            top_cat = max(expenses, key=expenses.get)
            lines.append(f"\n🏆 Больше всего: {top_cat} ({expenses[top_cat]:.2f}€)")
        if balance < 0:
            lines.append(f"⚠️ Расходы превысили доходы на {abs(balance):.2f}€")
        else:
            lines.append(f"✨ Сэкономлено {balance:.2f}€")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Ошибка анализа: {str(e)}")


async def monthly_analysis(context: ContextTypes.DEFAULT_TYPE):
    await send_monthly_report(context, context.job.chat_id)


async def weekly_sync(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    try:
        transactions = fetch_mono_transactions(days=7)
        added = save_to_sheet(transactions)
        now = datetime.now()
        expenses, income = get_month_data(now.month, now.year)
        total_exp = sum(expenses.values())
        total_inc = sum(income.values())
        balance = total_inc - total_exp
        lines = [
            f"📅 Еженедельная сводка — {MONTH_NAMES[now.month]} {now.year}\n",
            f"Monobank: загружено {added} новых транзакций\n",
            f"🔴 Расходы: {total_exp:.2f} €",
            f"🟢 Доходы: {total_inc:.2f} €",
            f"💰 Баланс: {'+' if balance >= 0 else ''}{balance:.2f} €",
            "\n📎 Не забудь загрузить CSV/PDF из Revolut, PayPal и Sparkasse!"
        ]
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Ошибка: {str(e)}")


async def setup_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    context.job_queue.run_repeating(weekly_sync, interval=timedelta(weeks=1), first=timedelta(seconds=5), chat_id=chat_id, name=f"weekly_{chat_id}")
    await update.message.reply_text("✅ Еженедельная синхронизация настроена! Каждое воскресенье буду присылать сводку 📊")


async def setup_monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    context.job_queue.run_monthly(monthly_analysis, when=datetime.now().replace(hour=9, minute=0, second=0).time(), day=1, chat_id=chat_id, name=f"monthly_{chat_id}")
    await update.message.reply_text("✅ Ежемесячный отчёт включён! Каждое 1-е число в 9:00 буду присылать анализ прошедшего месяца 🗓")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("sync", sync_mono))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("month", month_cmd))
    app.add_handler(CommandHandler("year", year_cmd))
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(CommandHandler("income", income_cmd))
    app.add_handler(CommandHandler("weekly", setup_weekly))
    app.add_handler(CommandHandler("monthly", setup_monthly))
    app.add_handler(CommandHandler("debug", debug))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(handle_callback))
    print("Бот запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
