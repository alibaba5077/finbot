import os
import io
import re
import json
import hashlib
import requests
import traceback
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
MONOBANK_TOKEN = os.getenv("MONOBANK_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

pending_files = {}

CATEGORIES = {
    "Продукты":           ["rewe", "lidl", "aldi", "edeka", "penny", "netto", "kaufland", "billa", "spar", "metro", "продукты", "market"],
    "Кафе":               ["cafe", "coffee", "starbucks", "kaffee", "restaurant", "pizza", "burger", "mcdonalds", "kfc", "subway", "bar", "кафе", "ресторан", "lieferando", "vending", "automat tas"],
    "Транспорт":          ["bvg", "db ", "deutsche bahn", "ubahn", "s-bahn", "uber", "bolt", "taxi", "mvg", "проездной", "транспорт", "tankstelle", "aral", "shell"],
    "Здоровье":           ["apotheke", "pharmacy", "arzt", "doctor", "аптека", "psycholog", "линзы", "brillen", "rossmann", "petrishcheva", "петришева", "marinapetrischeva", "paypal *marina"],
    "Фитнесс":            ["fitness", "gym", "sport", "yoga", "fitnessstudio", "mcfit", "mc-fit", "rsg group"],
    "Одежда":             ["zara", "h&m", "primark", "c&a", "uniqlo", "zalando", "одежда", "vinted", "kleiderkreisel", "tkmaxx", "tk maxx"],
    "Связь плюс подписки":["netflix", "spotify", "amazon prime", "apple", "google", "симка", "vodafone", "telekom", "o2", "congstar", "abo", "entgeltabrechnung", "freenet"],
    "Немецкий":           ["schule", "kurs", "deutsch", "немецкий", "volkshochschule", "vhs", "ahso", "netz.schule", "ксенія", "ксения"],
    "Подарки":            ["подарок", "geschenk", "blumen", "цветы"],
    "Для дома":           ["ikea", "obi", "bauhaus", "hornbach", "saturn", "mediamarkt", "haushalt", "dm drogerie", "dm-drogerie"],
    "Уход":               ["friseur", "kosmetik", "beauty", "nails", "массаж"],
}

IGNORE_KEYWORDS = ["verfügung geldautomat", "geldautomat", "auszahlung", "ga nr"]

MONTH_NAMES = {
    1: "янв.", 2: "февр.", 3: "март", 4: "апр.",
    5: "май", 6: "июн.", 7: "июл.", 8: "авг.",
    9: "сент.", 10: "окт.", 11: "нояб.", 12: "дек."
}

CAT_MAP = {
    "продукты": "Продукты", "еда": "Продукты",
    "кафе": "Кафе", "ресторан": "Кафе", "кофе": "Кафе",
    "транспорт": "Транспорт", "метро": "Транспорт",
    "здоровье": "Здоровье", "врач": "Здоровье", "аптека": "Здоровье", "психолог": "Здоровье",
    "фитнесс": "Фитнесс", "спорт": "Фитнесс", "фитнес": "Фитнесс",
    "одежда": "Одежда",
    "немецкий": "Немецкий", "курсы": "Немецкий",
    "подарки": "Подарки", "подарок": "Подарки",
    "дом": "Для дома",
    "уход": "Уход",
    "прочее": "Прочее",
}

UA_MONTHS = {
    "січ": 1, "лют": 2, "бер": 3, "квіт": 4, "трав": 5, "черв": 6,
    "лип": 7, "серп": 8, "вер": 9, "жовт": 10, "лист": 11, "груд": 12
}


def get_category(desc):
    d = desc.lower()
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            if kw in d:
                return cat
    return "Прочее"


def get_google_sheet():
    creds_json = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def parse_date(val):
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(str(val)[:10], fmt)
        except ValueError:
            continue
    return None


def parse_revolut_date(s):
    m = re.match(r"(\d{1,2})\s+(\w+)\.?\s+(\d{4})", s)
    if m:
        day, mon, yr = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        for key, num in UA_MONTHS.items():
            if key in mon:
                return datetime(yr, num, day)
    return None


def parse_amount(val):
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(" ", "")
    if "," in s:
        s = s.replace(",", ".")
    return float(s)


def get_eur_rate():
    try:
        r = requests.get("https://api.monobank.ua/bank/currency", timeout=5)
        for rate in r.json():
            if rate.get("currencyCodeA") == 978 and rate.get("currencyCodeB") == 980:
                return rate.get("rateSell") or rate.get("rateCross") or 45.0
    except Exception:
        pass
    return 45.0


def to_gs_date(dt):
    return (dt.date() if hasattr(dt, "date") else dt) - date(1899, 12, 30)


def save_to_sheet(transactions):
    if not transactions:
        return 0
    sheet = get_google_sheet()
    try:
        ws = sheet.worksheet("Повседневные")
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet("Повседневные", rows=1000, cols=7)
        ws.append_row(["Дата", "Месяц", "Категория", "Тип", "Стоимость", "Комментарий", "Источник"])

    all_data = ws.get_all_records()
    existing = set()
    for r in all_data:
        dv = str(r.get("Дата", "")).strip()
        av = str(r.get("Стоимость", "")).strip()
        tv = str(r.get("Тип", "")).strip()
        cv = str(r.get("Комментарий", "")).strip()
        if dv:
            existing.add(f"{dv}|{av}|{tv}")
            existing.add(f"{dv}|{cv}")

    added = 0
    for t in transactions:
        d = t["date"]
        gs_date = to_gs_date(d).days
        month = MONTH_NAMES[d.month]
        comment = t.get("description", "")
        amt = round(t["amount"], 2)
        tip = t.get("type", "Расход")
        date_str = d.strftime("%d.%m.%Y")

        m1 = f"{date_str}|{amt}|{tip}"
        m2 = f"{date_str}|{comment}"
        if m1 in existing or m2 in existing:
            continue

        ws.append_row([gs_date, month, t.get("category", ""), tip, amt, comment, t.get("source", "")])
        existing.add(m1)
        existing.add(m2)
        added += 1
    return added


def fetch_mono(days=7):
    now = int(datetime.now().timestamp())
    from_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    r = requests.get(
        f"https://api.monobank.ua/personal/statement/0/{from_ts}/{now}",
        headers={"X-Token": MONOBANK_TOKEN}
    )
    if r.status_code != 200:
        return []
    rate = get_eur_rate()
    result = []
    for t in r.json():
        if t["amount"] >= 0:
            continue
        cur = t.get("currencyCode", 980)
        if cur == 978:
            amt = abs(t.get("operationAmount", t["amount"])) / 100
        else:
            amt = round(abs(t["amount"]) / 100 / rate, 2)
        desc = t.get("description", "")
        dt = datetime.fromtimestamp(t["time"])
        result.append({"date": dt, "description": desc, "amount": amt,
                        "category": get_category(desc), "source": "Monobank", "type": "Расход"})
    return result


def parse_csv_revolut(text):
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    result = []
    for line in lines[1:]:
        parts = [p.strip().strip('"') for p in line.split(",")]
        try:
            dt = datetime.strptime(parts[0][:10], "%Y-%m-%d")
            desc = parts[2] if len(parts) > 2 else ""
            amt = float(parts[3]) if len(parts) > 3 else 0
            if amt >= 0:
                continue
            result.append({"date": dt, "description": desc, "amount": abs(amt),
                            "category": get_category(desc), "source": "Revolut", "type": "Расход"})
        except (ValueError, IndexError):
            continue
    return result


def parse_csv_paypal(text):
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    if not lines:
        return []
    sep = "\t" if "\t" in lines[0] else ","
    header = [h.strip().lower() for h in lines[0].split(sep)]
    try:
        date_idx = next(i for i, h in enumerate(header) if "datum" in h or "date" in h)
        desc_idx = next(i for i, h in enumerate(header) if "beschreibung" in h or "description" in h)
        amt_idx  = next(i for i, h in enumerate(header) if "brutto" in h or "gross" in h or "amount" in h)
    except StopIteration:
        date_idx, desc_idx, amt_idx = 0, 3, 5
    result = []
    for line in lines[1:]:
        parts = [p.strip().strip('"') for p in line.split(sep)]
        try:
            dt = parse_date(parts[date_idx])
            if not dt:
                continue
            desc = parts[desc_idx] if len(parts) > desc_idx else ""
            s = parts[amt_idx] if len(parts) > amt_idx else "0"
            amt = float(s.replace(".", "").replace(",", ".").replace(" ", ""))
            if amt >= 0:
                continue
            result.append({"date": dt, "description": desc, "amount": abs(amt),
                            "category": get_category(desc), "source": "PayPal", "type": "Расход"})
        except (ValueError, IndexError):
            continue
    return result


def parse_csv_sparkasse(text):
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    result = []
    for line in lines[1:]:
        parts = line.split(";")
        try:
            dt = parse_date(parts[0].strip())
            if not dt:
                continue
            desc = " ".join(p.strip().strip('"') for p in parts[1:-1] if p.strip())
            s = parts[-1].strip().strip('"').replace(" ", "")
            amt = float(s.replace(".", "").replace(",", "."))
            tip = "Расход" if amt < 0 else "Доход"
            result.append({"date": dt, "description": desc, "amount": abs(amt),
                            "category": get_category(desc) if tip == "Расход" else "",
                            "source": "Sparkasse", "type": tip})
        except (ValueError, IndexError):
            continue
    return result


def parse_pdf_sparkasse(pdf_bytes):
    try:
        import pdfplumber
    except ImportError:
        return []
    LINE_RE = re.compile(r"^(\d{2}\.\d{2}\.\d{4})(.+?)\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*$")
    SKIP_RE = re.compile(
        r"Kontostand|Seite|Berliner Sparkasse|Sparkassen|Vorstand|Telefon|www\.|BLZ:|SWIFT|"
        r"Amtsgericht|Vors\.|Postanschrift|Niederlassung|Sitz Berlin|USt|Rechnungsnummer|"
        r"Hinweise|Einwendungen|Gutschriften|Schecks|Sparurkunde|Dieser|Unsere|"
        r"Mit freundlichen|Ihre Sparkasse", re.IGNORECASE)
    result = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            all_lines = []
            for page in pdf.pages:
                all_lines.extend((page.extract_text() or "").split("\n"))
        i = 0
        while i < len(all_lines):
            line = all_lines[i].strip()
            if SKIP_RE.search(line):
                i += 1
                continue
            m = LINE_RE.match(line)
            if m:
                date_str, erl = m.group(1), m.group(2).strip()
                amt = float(m.group(3).replace(".", "").replace(",", "."))
                desc_parts = [erl]
                i += 1
                while i < len(all_lines):
                    nl = all_lines[i].strip()
                    if LINE_RE.match(nl) or SKIP_RE.search(nl):
                        break
                    if nl:
                        desc_parts.append(nl)
                    i += 1
                desc = " ".join(desc_parts)
                dt = parse_date(date_str)
                if not dt:
                    continue
                if any(kw in desc.lower() for kw in IGNORE_KEYWORDS):
                    continue
                tip = "Расход" if amt < 0 else "Доход"
                result.append({"date": dt, "description": desc, "amount": abs(amt),
                                "category": get_category(desc) if tip == "Расход" else "",
                                "source": "Sparkasse PDF", "type": tip})
            else:
                i += 1
    except Exception:
        pass
    return result


def parse_pdf_revolut(pdf_bytes):
    try:
        import pdfplumber
    except ImportError:
        return []
    DATE_RE = re.compile(r"^(\d{1,2}\s+\w+\.?\s+\d{4}р?\.?)\s+(.+?)\s+([\d\s]+,\d{2})€")
    AMT_RE  = re.compile(r"([\d\s]+,\d{2})€")
    SKIP_RE = re.compile(
        r"Revolut|IBAN|BIC|Seddiner|^Berlin$|Підсумок|Продукт|Рахунок|Всього|"
        r"Баланс у|^Операції|^Дата\s|^Опис\s|Повідомити|Отримати|Сканувати|"
        r"©|реєстрі|Ave\.|Депозити|документом|установу|відвідайте|розгляді|"
        r"центральним|Конституції|Вільнюс", re.IGNORECASE)
    result = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            all_lines = []
            for page in pdf.pages:
                all_lines.extend((page.extract_text() or "").split("\n"))
        i = 0
        while i < len(all_lines):
            line = all_lines[i].strip()
            if SKIP_RE.search(line):
                i += 1
                continue
            m = DATE_RE.match(line)
            if m:
                dt = parse_revolut_date(m.group(1))
                desc = m.group(2).strip()
                amounts = [float(a.replace(" ", "").replace(",", ".")) for a in AMT_RE.findall(line)]
                i += 1
                while i < len(all_lines):
                    nl = all_lines[i].strip()
                    if DATE_RE.match(nl) or SKIP_RE.search(nl):
                        break
                    if nl.startswith("Реквізити:"):
                        detail = nl.replace("Реквізити:", "").strip()
                        if detail:
                            desc += " " + detail
                    elif not nl.startswith("Від:"):
                        if nl:
                            desc += " " + nl
                    i += 1
                if not dt or not amounts:
                    continue
                # Пропускаем собственные переводы
                if "alina dotsenko" in desc.lower():
                    continue
                # Входящий платёж = доход
                if "платіж від" in desc.lower():
                    tip, amt = "Доход", amounts[0]
                else:
                    tip, amt = "Расход", amounts[0]
                result.append({"date": dt, "description": desc.strip(), "amount": amt,
                                "category": get_category(desc) if tip == "Расход" else "",
                                "source": "Revolut PDF", "type": tip})
            else:
                i += 1
    except Exception:
        pass
    return result


def get_month_data(month, year):
    ws = get_google_sheet().worksheet("Повседневные")
    expenses, income = {}, {}
    for r in ws.get_all_records():
        dv = str(r.get("Дата", "")).strip()
        if not dv:
            continue
        dt = parse_date(dv)
        if not dt or dt.month != month or dt.year != year:
            continue
        try:
            amt = parse_amount(r.get("Стоимость", 0))
        except ValueError:
            continue
        if r.get("Тип") == "Расход":
            cat = r.get("Категория") or "Прочее"
            expenses[cat] = expenses.get(cat, 0) + amt
        elif r.get("Тип") == "Доход":
            cat = r.get("Категория") or "Доход"
            income[cat] = income.get(cat, 0) + amt
    return expenses, income


# ── Handlers ─────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я твой финансовый бот.\n\n"
        "📥 /sync — Monobank за 7 дней\n"
        "📊 /summary — текущий месяц\n"
        "📅 /month 04.2026 — любой месяц\n"
        "📈 /year 2026 — годовой итог\n"
        "➕ /add 25.5 продукты lidl\n"
        "💚 /income 563 выплата\n"
        "🔔 /weekly — еженедельная синхронизация\n"
        "🗓 /monthly — ежемесячный отчёт\n"
        "📎 Отправь CSV или PDF файл"
    )


async def cmd_sync(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Загружаю из Monobank...")
    try:
        txs = fetch_mono(days=7)
        if not txs:
            await update.message.reply_text("✅ Новых транзакций нет.")
            return
        added = save_to_sheet(txs)
        by_cat = {}
        for t in txs:
            by_cat[t["category"]] = by_cat.get(t["category"], 0) + t["amount"]
        lines = [f"✅ Загружено {added} новых из {len(txs)}!\n", "📊 По категориям:"]
        for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
            lines.append(f"  • {cat}: {amt:.2f} €")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ {e}\n{traceback.format_exc()[-300:]}")


async def show_summary(update, month, year):
    mn = MONTH_NAMES[month]
    await update.message.reply_text(f"⏳ Считаю {mn} {year}...")
    try:
        exp, inc = get_month_data(month, year)
        te, ti = sum(exp.values()), sum(inc.values())
        if not exp and not inc:
            await update.message.reply_text(f"Данных за {mn} {year} нет.")
            return
        lines = [f"📊 {mn} {year}\n"]
        if exp:
            lines.append("🔴 РАСХОДЫ:")
            for cat, amt in sorted(exp.items(), key=lambda x: -x[1]):
                lines.append(f"  • {cat}: {amt:.2f}€ ({amt/te*100:.0f}%)")
            lines.append(f"  Итого: {te:.2f}€")
        if inc:
            lines.append("\n🟢 ДОХОДЫ:")
            for cat, amt in sorted(inc.items(), key=lambda x: -x[1]):
                lines.append(f"  • {cat}: {amt:.2f}€")
            lines.append(f"  Итого: {ti:.2f}€")
        bal = ti - te
        lines.append(f"\n💰 Баланс: {'+' if bal>=0 else ''}{bal:.2f}€")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ {e}\n{traceback.format_exc()[-300:]}")


async def cmd_summary(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    n = datetime.now()
    await show_summary(update, n.month, n.year)


async def cmd_month(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Пример: /month 04.2026")
        return
    try:
        p = ctx.args[0].split(".")
        await show_summary(update, int(p[0]), int(p[1]))
    except Exception:
        await update.message.reply_text("❌ Формат: /month 04.2026")


async def cmd_year(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    year = int(ctx.args[0]) if ctx.args else datetime.now().year
    await update.message.reply_text(f"⏳ Считаю {year}...")
    try:
        ws = get_google_sheet().worksheet("Повседневные")
        monthly = {}
        for r in ws.get_all_records():
            dv = str(r.get("Дата", "")).strip()
            if not dv:
                continue
            dt = parse_date(dv)
            if not dt or dt.year != year:
                continue
            try:
                amt = parse_amount(r.get("Стоимость", 0))
            except ValueError:
                continue
            m = dt.month
            monthly.setdefault(m, {"exp": 0, "inc": 0})
            if r.get("Тип") == "Расход":
                monthly[m]["exp"] += amt
            elif r.get("Тип") == "Доход":
                monthly[m]["inc"] += amt
        if not monthly:
            await update.message.reply_text(f"Данных за {year} нет.")
            return
        lines = [f"📈 Итоги {year}\n"]
        te = ti = 0
        for m in sorted(monthly):
            e, i = monthly[m]["exp"], monthly[m]["inc"]
            te += e; ti += i
            b = i - e
            lines.append(f"{MONTH_NAMES[m]:6} 🔴{e:.0f}€ 🟢{i:.0f}€ {'+'if b>=0 else''}{b:.0f}€")
        lines.append("─" * 32)
        b = ti - te
        lines.append(f"Итого  🔴{te:.2f}€ 🟢{ti:.2f}€ {'+'if b>=0 else''}{b:.2f}€")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 2:
        await update.message.reply_text(
            "Формат: /add сумма категория описание\n"
            "Пример: /add 25.5 продукты lidl"
        )
        return
    try:
        amt = float(ctx.args[0].replace(",", "."))
        cat = CAT_MAP.get(ctx.args[1].lower(), get_category(ctx.args[1]))
        desc = " ".join(ctx.args[2:]) if len(ctx.args) > 2 else ctx.args[1]
        now = datetime.now()
        save_to_sheet([{"date": now, "description": desc, "amount": amt,
                         "category": cat, "source": "Бот", "type": "Расход"}])
        await update.message.reply_text(
            f"✅ Добавлено!\n  📁 {cat}\n  💶 {amt:.2f}€\n  📝 {desc}\n  📅 {now.strftime('%d.%m.%Y')}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_income(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 2:
        await update.message.reply_text("Пример: /income 563 выплата")
        return
    try:
        amt = float(ctx.args[0].replace(",", "."))
        desc = " ".join(ctx.args[1:])
        now = datetime.now()
        save_to_sheet([{"date": now, "description": desc, "amount": amt,
                         "category": "", "source": "Бот", "type": "Доход"}])
        await update.message.reply_text(
            f"✅ Доход добавлен!\n  🟢 {amt:.2f}€\n  📝 {desc}\n  📅 {now.strftime('%d.%m.%Y')}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_debug(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        ws = get_google_sheet().worksheet("Повседневные")
        data = ws.get_all_records()
        now = datetime.now()
        rows = [r for r in data if str(r.get("Дата","")).strip() and
                parse_date(str(r.get("Дата",""))) and
                parse_date(str(r.get("Дата",""))).month == now.month]
        lines = [f"Строк всего: {len(data)}", f"За текущий месяц: {len(rows)}", ""]
        for r in rows[:5]:
            lines.append(f"  {r.get('Дата','')} | {r.get('Категория','')} | {r.get('Тип','')} | {r.get('Стоимость','')}({type(r.get('Стоимость','')).__name__})")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().lower()
    parts = text.split()
    if len(parts) < 2:
        return
    amt = None
    cat_input = None
    rest = []
    for i, p in enumerate(parts):
        try:
            amt = float(p.replace(",", "."))
            remaining = parts[:i] + parts[i+1:]
            cat_input = remaining[0] if remaining else "прочее"
            rest = remaining[1:]
            break
        except ValueError:
            continue
    if amt is None:
        return
    cat = CAT_MAP.get(cat_input, get_category(cat_input))
    desc = " ".join(rest) if rest else cat_input
    now = datetime.now()
    try:
        save_to_sheet([{"date": now, "description": desc, "amount": amt,
                         "category": cat, "source": "Бот", "type": "Расход"}])
        await update.message.reply_text(
            f"✅ Добавлено!\n  📁 {cat}\n  💶 {amt:.2f}€\n  📝 {desc}\n  📅 {now.strftime('%d.%m.%Y')}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    fname = doc.file_name.lower()
    is_pdf = fname.endswith(".pdf")
    is_csv = fname.endswith(".csv")

    if not is_pdf and not is_csv:
        await update.message.reply_text("Пожалуйста, отправь CSV или PDF файл.")
        return

    if "revolut" in fname or "account-statement" in fname:
        source = "Revolut"
    elif "paypal" in fname or "csr" in fname:
        source = "PayPal"
    elif any(x in fname for x in ["sparkasse", "umsatz", "export", "konto", "auszug"]):
        source = "Sparkasse"
    else:
        key = hashlib.md5(doc.file_id.encode()).hexdigest()[:8]
        pending_files[key] = (doc.file_id, is_pdf)
        fmt = "PDF" if is_pdf else "CSV"
        kb = [[InlineKeyboardButton("Revolut",    callback_data=f"file_revolut_{key}")],
              [InlineKeyboardButton("PayPal",     callback_data=f"file_paypal_{key}")],
              [InlineKeyboardButton("Sparkasse",  callback_data=f"file_sparkasse_{key}")]]
        await update.message.reply_text(f"Из какого банка этот {fmt}?",
                                         reply_markup=InlineKeyboardMarkup(kb))
        return

    await process_file(update, ctx, doc.file_id, source, is_pdf)


async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split("_", 2)
    if parts[0] == "file" and len(parts) == 3:
        src_map = {"revolut": "Revolut", "paypal": "PayPal", "sparkasse": "Sparkasse"}
        source = src_map.get(parts[1], parts[1])
        key = parts[2]
        if key not in pending_files:
            await q.edit_message_text("❌ Файл устарел, отправь заново.")
            return
        file_id, is_pdf = pending_files.pop(key)
        await process_file(q, ctx, file_id, source, is_pdf)


async def process_file(upd, ctx, file_id, source, is_pdf):
    reply = upd.message.reply_text if hasattr(upd, "message") else upd.edit_message_text
    await reply(f"⏳ Обрабатываю {source}...")
    try:
        f = await ctx.bot.get_file(file_id)
        raw = bytes(await f.download_as_bytearray())

        if is_pdf:
            txs = parse_pdf_revolut(raw) if source == "Revolut" else parse_pdf_sparkasse(raw)
        else:
            text = raw.decode("utf-8", errors="ignore")
            if source == "Revolut":
                txs = parse_csv_revolut(text)
            elif source == "PayPal":
                txs = parse_csv_paypal(text)
            else:
                txs = parse_csv_sparkasse(text)

        if not txs:
            await reply("❌ Транзакции не найдены. Проверь формат файла.")
            return

        added = save_to_sheet(txs)
        by_cat = {}
        for t in txs:
            if t.get("type") == "Расход":
                by_cat[t["category"]] = by_cat.get(t["category"], 0) + t["amount"]

        lines = [f"✅ {source}: загружено {added} новых из {len(txs)}!\n"]
        if by_cat:
            lines.append("📊 Расходы:")
            for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
                lines.append(f"  • {cat}: {amt:.2f}€")
        await reply("\n".join(lines))
    except Exception as e:
        await reply(f"❌ Ошибка: {e}\n{traceback.format_exc()[-400:]}")


async def weekly_job(ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = ctx.job.chat_id
    try:
        txs = fetch_mono(days=7)
        added = save_to_sheet(txs)
        now = datetime.now()
        exp, inc = get_month_data(now.month, now.year)
        te, ti = sum(exp.values()), sum(inc.values())
        bal = ti - te
        lines = [f"📅 Еженедельная сводка — {MONTH_NAMES[now.month]} {now.year}\n",
                 f"Monobank: {added} новых транзакций\n",
                 f"🔴 Расходы: {te:.2f}€",
                 f"🟢 Доходы: {ti:.2f}€",
                 f"💰 Баланс: {'+' if bal>=0 else ''}{bal:.2f}€",
                 "\n📎 Не забудь загрузить CSV/PDF из Revolut, PayPal и Sparkasse!"]
        await ctx.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        await ctx.bot.send_message(chat_id=chat_id, text=f"❌ {e}")


async def monthly_job(ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = ctx.job.chat_id
    now = datetime.now()
    month = now.month - 1 if now.month > 1 else 12
    year = now.year if now.month > 1 else now.year - 1
    try:
        exp, inc = get_month_data(month, year)
        te, ti = sum(exp.values()), sum(inc.values())
        bal = ti - te
        mn = MONTH_NAMES[month]
        lines = [f"🗓 Отчёт — {mn} {year}\n"]
        if exp:
            lines.append("🔴 Расходы:")
            for cat, amt in sorted(exp.items(), key=lambda x: -x[1]):
                lines.append(f"  • {cat}: {amt:.2f}€ ({amt/te*100:.0f}%)")
            lines.append(f"  Итого: {te:.2f}€\n")
        if inc:
            lines.append("🟢 Доходы:")
            for cat, amt in sorted(inc.items(), key=lambda x: -x[1]):
                lines.append(f"  • {cat}: {amt:.2f}€")
            lines.append(f"  Итого: {ti:.2f}€\n")
        lines.append(f"💰 Баланс: {'+' if bal>=0 else ''}{bal:.2f}€")
        if exp:
            top = max(exp, key=exp.get)
            lines.append(f"🏆 Топ трата: {top} ({exp[top]:.2f}€)")
        lines.append("✨ Сэкономлено!" if bal >= 0 else f"⚠️ Перерасход на {abs(bal):.2f}€")
        await ctx.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        await ctx.bot.send_message(chat_id=chat_id, text=f"❌ {e}")


async def cmd_weekly(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ctx.job_queue.run_repeating(weekly_job, interval=timedelta(weeks=1),
                                 first=timedelta(seconds=5), chat_id=chat_id,
                                 name=f"weekly_{chat_id}")
    await update.message.reply_text("✅ Еженедельная синхронизация включена!")


async def cmd_monthly(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ctx.job_queue.run_monthly(monthly_job,
                               when=datetime.now().replace(hour=9, minute=0, second=0).time(),
                               day=1, chat_id=chat_id, name=f"monthly_{chat_id}")
    await update.message.reply_text("✅ Ежемесячный отчёт включён! Каждое 1-е число в 9:00 🗓")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("sync",    cmd_sync))
    app.add_handler(CommandHandler("summary", cmd_summary))
    app.add_handler(CommandHandler("month",   cmd_month))
    app.add_handler(CommandHandler("year",    cmd_year))
    app.add_handler(CommandHandler("add",     cmd_add))
    app.add_handler(CommandHandler("income",  cmd_income))
    app.add_handler(CommandHandler("weekly",  cmd_weekly))
    app.add_handler(CommandHandler("monthly", cmd_monthly))
    app.add_handler(CommandHandler("debug",   cmd_debug))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))
    print("Бот запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
