# ============================================================
#  Продавець AI — api_server.py (версия 2: склад и недостачи)
#  FastAPI + PostgreSQL (Railway)
#  Что умеет:
#   - товары: добавить / список / изменить / удалить
#   - продажи: пробить продажу (списывает остаток)
#   - ПРИХОД товара на склад (+ история приходов)          [НОВОЕ]
#   - СВЕРКА остатков: контроль недостач                   [НОВОЕ]
#   - статистика: за сегодня и за период
#   - лента последних продаж (для живого дашборда)
#   - AI-советник (видит и недостачи)
# ============================================================

import asyncio
import json
import math
import os
import random
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# AI-директор: бот пишет владельцу сам (задай обе переменные в Railway)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "").strip()

app = FastAPI(title="Продавець AI")

# Разрешаем запросы из браузера (Mini App на GitHub Pages)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

pool = None  # пул соединений с базой

# Киевское время (UTC+3 летом; для MVP достаточно фиксированного сдвига)
KYIV = timezone(timedelta(hours=3))


def today_bounds():
    """Начало и конец сегодняшнего дня по Киеву (в UTC для запросов к базе)."""
    now_kyiv = datetime.now(KYIV)
    start = now_kyiv.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


# ------------------------------------------------------------
#  Старт: подключаемся к базе и создаём таблицы
# ------------------------------------------------------------
@app.on_event("startup")
async def startup():
    global pool
    if not DATABASE_URL:
        print("ВНИМАНИЕ: переменная DATABASE_URL не задана!")
        return
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as con:
        await con.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                buy_price NUMERIC(12,2) NOT NULL DEFAULT 0,   -- цена закупки
                sell_price NUMERIC(12,2) NOT NULL DEFAULT 0,  -- цена продажи
                stock INTEGER NOT NULL DEFAULT 0,             -- остаток на складе
                min_stock INTEGER NOT NULL DEFAULT 3,         -- порог "заканчивается"
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                qty INTEGER NOT NULL,
                sell_price NUMERIC(12,2) NOT NULL,  -- цена на момент продажи
                buy_price NUMERIC(12,2) NOT NULL,   -- закупка на момент продажи
                sold_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        await con.execute(
            "CREATE INDEX IF NOT EXISTS idx_sales_sold_at ON sales(sold_at);"
        )
        # НОВОЕ: история приходов товара на склад
        await con.execute("""
            CREATE TABLE IF NOT EXISTS receipts (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                qty INTEGER NOT NULL,
                buy_price NUMERIC(12,2) NOT NULL DEFAULT 0,  -- закупка этой партии
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        # НОВОЕ: история сверок остатков (контроль недостач)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS inventory_checks (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                expected INTEGER NOT NULL,   -- сколько должно быть по системе
                actual INTEGER NOT NULL,     -- сколько реально на полке
                diff INTEGER NOT NULL,       -- разница (минус = недостача)
                checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        # НОВОЕ: фото товара (добавляем колонку, если её ещё нет)
        await con.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS img TEXT;")
        # НОВОЕ: штрих-код товара
        await con.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS barcode TEXT;")
        # НОВОЕ: служебное состояние (дата последнего утреннего отчёта и т.п.)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        # НОВОЕ: флаг "уже предупреждали, что товар заканчивается"
        await con.execute(
            "ALTER TABLE products ADD COLUMN IF NOT EXISTS low_notified BOOLEAN DEFAULT FALSE;"
        )
    print("База готова: products, sales, receipts, inventory_checks на месте")
    # НОВОЕ: запускаем фоновый цикл AI-директора (утренний отчёт + тревоги)
    asyncio.create_task(director_loop())


@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()


# ------------------------------------------------------------
#  Модели входных данных
# ------------------------------------------------------------
class ProductIn(BaseModel):
    name: str
    buy_price: float = 0
    sell_price: float = 0
    stock: int = 0
    min_stock: int = 3
    img: Optional[str] = None  # фото товара (сжатая data:image строка)
    barcode: Optional[str] = None  # штрих-код (EAN и т.п.)


class SaleIn(BaseModel):
    product_id: int
    qty: int = 1


class ReceiptIn(BaseModel):
    product_id: int
    qty: int = 1
    buy_price: Optional[float] = None  # новая цена закупки (можно не указывать)


class CheckItemIn(BaseModel):
    product_id: int
    actual: int  # сколько реально насчитали на полке


class InventoryIn(BaseModel):
    items: List[CheckItemIn]


# ------------------------------------------------------------
#  Служебное
# ------------------------------------------------------------
@app.get("/")
async def root():
    return {"app": "Продавець AI", "status": "ok"}


# ------------------------------------------------------------
#  ТОВАРЫ
# ------------------------------------------------------------
@app.get("/products")
async def list_products():
    """Список всех товаров + пометка low (заканчивается)."""
    async with pool.acquire() as con:
        rows = await con.fetch(
            "SELECT * FROM products ORDER BY name;"
        )
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "buy_price": float(r["buy_price"]),
            "sell_price": float(r["sell_price"]),
            "stock": r["stock"],
            "min_stock": r["min_stock"],
            "img": r["img"],
            "barcode": r["barcode"],
            "low": r["stock"] <= r["min_stock"],
        }
        for r in rows
    ]


@app.post("/products")
async def add_product(p: ProductIn):
    """Добавить товар."""
    name = p.name.strip()
    if not name:
        raise HTTPException(400, "Название не может быть пустым")
    if p.img and len(p.img) > 400000:
        raise HTTPException(400, "Фото завелике, спробуй інше")
    barcode = (p.barcode or "").strip() or None
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """INSERT INTO products (name, buy_price, sell_price, stock, min_stock, img, barcode)
               VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id;""",
            name, p.buy_price, p.sell_price, p.stock, p.min_stock, p.img, barcode,
        )
    return {"ok": True, "id": row["id"]}


@app.put("/products/{product_id}")
async def update_product(product_id: int, p: ProductIn):
    """Изменить товар (название, цены, остаток, порог, фото, штрих-код)."""
    if p.img and len(p.img) > 400000:
        raise HTTPException(400, "Фото завелике, спробуй інше")
    barcode = (p.barcode or "").strip() or None
    async with pool.acquire() as con:
        result = await con.execute(
            """UPDATE products
               SET name=$1, buy_price=$2, sell_price=$3, stock=$4, min_stock=$5, img=$6, barcode=$7
               WHERE id=$8;""",
            p.name.strip(), p.buy_price, p.sell_price, p.stock, p.min_stock, p.img, barcode,
            product_id,
        )
    if result == "UPDATE 0":
        raise HTTPException(404, "Товар не найден")
    return {"ok": True}


@app.delete("/products/{product_id}")
async def delete_product(product_id: int):
    """Удалить товар (и его историю продаж)."""
    async with pool.acquire() as con:
        result = await con.execute(
            "DELETE FROM products WHERE id=$1;", product_id
        )
    if result == "DELETE 0":
        raise HTTPException(404, "Товар не найден")
    return {"ok": True}


# ------------------------------------------------------------
#  ПРОДАЖИ
# ------------------------------------------------------------
@app.post("/sales")
async def make_sale(s: SaleIn):
    """Пробить продажу: списывает остаток и пишет запись в историю."""
    if s.qty <= 0:
        raise HTTPException(400, "Количество должно быть больше нуля")
    async with pool.acquire() as con:
        async with con.transaction():
            product = await con.fetchrow(
                "SELECT * FROM products WHERE id=$1 FOR UPDATE;", s.product_id
            )
            if not product:
                raise HTTPException(404, "Товар не найден")
            if product["stock"] < s.qty:
                raise HTTPException(
                    400, f"Недостаточно на складе: есть {product['stock']}"
                )
            await con.execute(
                "UPDATE products SET stock = stock - $1 WHERE id=$2;",
                s.qty, s.product_id,
            )
            row = await con.fetchrow(
                """INSERT INTO sales (product_id, qty, sell_price, buy_price)
                   VALUES ($1, $2, $3, $4) RETURNING id, sold_at;""",
                s.product_id, s.qty, product["sell_price"], product["buy_price"],
            )
    total = float(product["sell_price"]) * s.qty
    return {"ok": True, "sale_id": row["id"], "total": total}


@app.get("/sales/recent")
async def recent_sales(limit: int = 20):
    """Лента последних продаж — для живого дашборда владельца."""
    limit = max(1, min(limit, 100))
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT s.id, s.qty, s.sell_price, s.sold_at, p.name
               FROM sales s JOIN products p ON p.id = s.product_id
               ORDER BY s.sold_at DESC LIMIT $1;""",
            limit,
        )
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "qty": r["qty"],
            "total": float(r["sell_price"]) * r["qty"],
            "time": r["sold_at"].astimezone(KYIV).strftime("%H:%M"),
            "date": r["sold_at"].astimezone(KYIV).strftime("%d.%m"),
        }
        for r in rows
    ]


# ------------------------------------------------------------
#  ПРИХОД ТОВАРА НА СКЛАД  [НОВОЕ]
# ------------------------------------------------------------
@app.post("/receipts")
async def add_receipt(rc: ReceiptIn):
    """Приход товара: увеличивает остаток и пишет запись в историю приходов.
    Если указана новая цена закупки — обновляет её у товара."""
    if rc.qty <= 0:
        raise HTTPException(400, "Кількість має бути більше нуля")
    async with pool.acquire() as con:
        async with con.transaction():
            product = await con.fetchrow(
                "SELECT * FROM products WHERE id=$1 FOR UPDATE;", rc.product_id
            )
            if not product:
                raise HTTPException(404, "Товар не знайдено")
            # Цена закупки партии: новая (если указали) или текущая
            price = float(product["buy_price"])
            if rc.buy_price is not None and rc.buy_price > 0:
                price = rc.buy_price
                await con.execute(
                    "UPDATE products SET buy_price=$1 WHERE id=$2;",
                    price, rc.product_id,
                )
            await con.execute(
                "UPDATE products SET stock = stock + $1 WHERE id=$2;",
                rc.qty, rc.product_id,
            )
            row = await con.fetchrow(
                """INSERT INTO receipts (product_id, qty, buy_price)
                   VALUES ($1, $2, $3) RETURNING id;""",
                rc.product_id, rc.qty, price,
            )
    return {
        "ok": True,
        "receipt_id": row["id"],
        "new_stock": product["stock"] + rc.qty,
    }


@app.get("/receipts/recent")
async def recent_receipts(limit: int = 20):
    """Лента последних приходов на склад."""
    limit = max(1, min(limit, 100))
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT r.id, r.qty, r.buy_price, r.received_at, p.name
               FROM receipts r JOIN products p ON p.id = r.product_id
               ORDER BY r.received_at DESC LIMIT $1;""",
            limit,
        )
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "qty": r["qty"],
            "sum": float(r["buy_price"]) * r["qty"],
            "time": r["received_at"].astimezone(KYIV).strftime("%H:%M"),
            "date": r["received_at"].astimezone(KYIV).strftime("%d.%m"),
        }
        for r in rows
    ]


# ------------------------------------------------------------
#  СВЕРКА ОСТАТКОВ: КОНТРОЛЬ НЕДОСТАЧ  [НОВОЕ]
# ------------------------------------------------------------
@app.post("/inventory/check")
async def inventory_check(inv: InventoryIn):
    """Сверка: сравнивает реальные остатки с системными.
    Записывает расхождения и обновляет остатки на реальные."""
    if not inv.items:
        raise HTTPException(400, "Список порожній")
    results = []
    async with pool.acquire() as con:
        async with con.transaction():
            for item in inv.items:
                if item.actual < 0:
                    continue
                product = await con.fetchrow(
                    "SELECT * FROM products WHERE id=$1 FOR UPDATE;",
                    item.product_id,
                )
                if not product:
                    continue
                expected = product["stock"]
                diff = item.actual - expected
                await con.execute(
                    """INSERT INTO inventory_checks (product_id, expected, actual, diff)
                       VALUES ($1, $2, $3, $4);""",
                    item.product_id, expected, item.actual, diff,
                )
                # Приводим системный остаток к реальному
                if diff != 0:
                    await con.execute(
                        "UPDATE products SET stock=$1 WHERE id=$2;",
                        item.actual, item.product_id,
                    )
                results.append({
                    "product_id": item.product_id,
                    "name": product["name"],
                    "expected": expected,
                    "actual": item.actual,
                    "diff": diff,
                    "loss": round(float(product["sell_price"]) * (-diff), 2) if diff < 0 else 0,
                })
    shortages = [r for r in results if r["diff"] < 0]
    surplus = [r for r in results if r["diff"] > 0]
    total_loss = round(sum(r["loss"] for r in shortages), 2)
    # AI-директор: мгновенно сообщаем владельцу о недостаче в Telegram
    if shortages:
        lines = "\n".join(
            "• " + r["name"] + ": −" + str(-r["diff"]) + " шт (₴" + str(r["loss"]) + ")"
            for r in shortages[:15]
        )
        asyncio.create_task(tg_send(
            "😱 <b>Звірка виявила недостачу!</b>\n" + lines +
            "\n\n💸 Втрати: <b>₴" + str(total_loss) + "</b> (за цінами продажу)"
        ))
    return {
        "ok": True,
        "checked": len(results),
        "shortages": shortages,       # недостачи
        "surplus": surplus,           # излишки
        "total_loss": total_loss,     # потери в ценах продажи, грн
        "results": results,
    }


@app.get("/inventory/recent")
async def recent_checks(limit: int = 30):
    """История расхождений (только где diff != 0) — для отчёта владельцу."""
    limit = max(1, min(limit, 200))
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT c.id, c.expected, c.actual, c.diff, c.checked_at, p.name
               FROM inventory_checks c JOIN products p ON p.id = c.product_id
               WHERE c.diff <> 0
               ORDER BY c.checked_at DESC LIMIT $1;""",
            limit,
        )
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "expected": r["expected"],
            "actual": r["actual"],
            "diff": r["diff"],
            "time": r["checked_at"].astimezone(KYIV).strftime("%H:%M"),
            "date": r["checked_at"].astimezone(KYIV).strftime("%d.%m"),
        }
        for r in rows
    ]


# ------------------------------------------------------------
#  ДЕМО-НАПОЛНЕНИЕ МАГАЗИНА  [ВРЕМЕННОЕ]
#  Открой https://.../seed-demo в браузере ОДИН раз —
#  создаст 22 реальных товара и продажи за 14 дней.
#  Работает только если магазин пустой (ничего не перезапишет).
# ------------------------------------------------------------

# (назва, закупка, продаж, залишок, мінімум, популярність)
DEMO_PRODUCTS = [
    ("Хліб «Український»",        16, 22, 24, 6, 10),
    ("Батон нарізний",            14, 20, 20, 6, 9),
    ("Молоко 2.5% 1л",            32, 42, 30, 8, 10),
    ("Кефір 900г",                33, 44, 18, 5, 6),
    ("Сметана 15% 350г",          38, 52, 15, 4, 5),
    ("Яйця С1, десяток",          42, 55, 25, 6, 8),
    ("Масло вершкове 200г",       68, 89, 12, 3, 4),
    ("Сир твердий 300г",          95, 125, 10, 3, 3),
    ("Ковбаса «Докторська» 400г", 88, 115, 12, 3, 5),
    ("Сосиски молочні 400г",      72, 95, 14, 4, 5),
    ("Кока-кола 0.5л",            17, 28, 48, 10, 9),
    ("Вода «Моршинська» 1.5л",    14, 24, 40, 10, 8),
    ("Пиво «Чернігівське» 0.5л",  22, 33, 36, 8, 7),
    ("Чіпси Lay's 120г",          38, 55, 22, 5, 6),
    ("Шоколад «Мілка» 90г",       42, 58, 18, 5, 5),
    ("Печиво «Марія» 300г",       26, 38, 16, 4, 4),
    ("Гречка 800г",               44, 62, 14, 4, 3),
    ("Макарони «Чумак» 400г",     24, 35, 20, 5, 4),
    ("Цукор 1кг",                 28, 39, 18, 5, 4),
    ("Олія соняшникова 850мл",    55, 72, 15, 4, 4),
    ("Туалетний папір, 4 рул.",   32, 45, 16, 4, 3),
    ("Пакет-майка",               1, 3, 100, 20, 10),
]


@app.get("/seed-demo")
async def seed_demo():
    """Наполняет пустой магазин демо-товарами и историей продаж за 14 дней."""
    async with pool.acquire() as con:
        cnt = await con.fetchval("SELECT COUNT(*) FROM products;")
        if cnt and cnt > 0:
            return {
                "ok": False,
                "message": f"У магазині вже є {cnt} товарів — сідер працює "
                           "тільки на порожній базі, щоб нічого не зіпсувати.",
            }
        # 1) Товары
        ids = []
        weights = []
        for name, buy, sell, stock, ms, w in DEMO_PRODUCTS:
            row = await con.fetchrow(
                """INSERT INTO products (name, buy_price, sell_price, stock, min_stock)
                   VALUES ($1, $2, $3, $4, $5) RETURNING id;""",
                name, buy, sell, stock, ms,
            )
            ids.append((row["id"], buy, sell))
            weights.append(w)
        # 2) Продажи за прошлые 14 дней (историю остаток не трогает)
        total_sales = 0
        now_kyiv = datetime.now(KYIV)
        for d in range(14, 0, -1):
            day = now_kyiv - timedelta(days=d)
            for _ in range(random.randint(15, 35)):
                idx = random.choices(range(len(ids)), weights=weights, k=1)[0]
                pid, buy, sell = ids[idx]
                qty = random.choice([1, 1, 1, 1, 2, 2, 3])
                sold = day.replace(
                    hour=random.randint(8, 20),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                ).astimezone(timezone.utc)
                await con.execute(
                    """INSERT INTO sales (product_id, qty, sell_price, buy_price, sold_at)
                       VALUES ($1, $2, $3, $4, $5);""",
                    pid, qty, sell, buy, sold,
                )
                total_sales += 1
        # 3) Немного продаж сегодня (до текущего часа), чтобы «Сьогодні» ожило
        if now_kyiv.hour >= 8:
            for _ in range(random.randint(4, 10)):
                idx = random.choices(range(len(ids)), weights=weights, k=1)[0]
                pid, buy, sell = ids[idx]
                qty = random.choice([1, 1, 1, 2])
                sold = now_kyiv.replace(
                    hour=random.randint(8, max(8, now_kyiv.hour)),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                ).astimezone(timezone.utc)
                await con.execute(
                    """INSERT INTO sales (product_id, qty, sell_price, buy_price, sold_at)
                       VALUES ($1, $2, $3, $4, $5);""",
                    pid, qty, sell, buy, sold,
                )
                total_sales += 1
    return {
        "ok": True,
        "products": len(ids),
        "sales": total_sales,
        "message": "Магазин наповнено! Відкривай Mini App 🛒",
    }


# ------------------------------------------------------------
#  СТАТИСТИКА
# ------------------------------------------------------------
@app.get("/stats/today")
async def stats_today():
    """Сегодня: выручка, прибыль, число продаж."""
    start, end = today_bounds()
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """SELECT
                 COALESCE(SUM(sell_price * qty), 0) AS revenue,
                 COALESCE(SUM((sell_price - buy_price) * qty), 0) AS profit,
                 COUNT(*) AS sales_count
               FROM sales WHERE sold_at >= $1 AND sold_at < $2;""",
            start, end,
        )
    return {
        "revenue": float(row["revenue"]),
        "profit": float(row["profit"]),
        "sales_count": row["sales_count"],
    }


@app.get("/stats/range")
async def stats_range(days: int = 7):
    """Обороты по дням за период (для графика). days=7/30/365."""
    days = max(1, min(days, 730))
    start = (datetime.now(KYIV) - timedelta(days=days - 1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(timezone.utc)
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT
                 DATE(sold_at AT TIME ZONE 'Europe/Kyiv') AS day,
                 COALESCE(SUM(sell_price * qty), 0) AS revenue,
                 COALESCE(SUM((sell_price - buy_price) * qty), 0) AS profit,
                 COUNT(*) AS sales_count
               FROM sales
               WHERE sold_at >= $1
               GROUP BY day ORDER BY day;""",
            start,
        )
    return [
        {
            "day": r["day"].strftime("%d.%m"),
            "revenue": float(r["revenue"]),
            "profit": float(r["profit"]),
            "sales_count": r["sales_count"],
        }
        for r in rows
    ]


# ------------------------------------------------------------
#  AI-СОВЕТНИК
# ------------------------------------------------------------
import httpx

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()


class AdvisorIn(BaseModel):
    question: str
    history: list = []  # [{"role": "user"/"assistant", "content": "..."}]


def extract_text(data: dict) -> str:
    """Достаёт текст из ответа модели (пропускает thinking-блоки)."""
    parts = []
    for block in data.get("content", []):
        if block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "\n".join(parts).strip()


async def collect_store_data() -> str:
    """Собирает сводку по магазину для AI: товары, сегодня, 30 дней, топы, недостачи."""
    start, end = today_bounds()
    month_start = (datetime.now(KYIV) - timedelta(days=29)).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(timezone.utc)

    async with pool.acquire() as con:
        prods = await con.fetch(
            "SELECT name, buy_price, sell_price, stock, min_stock FROM products ORDER BY name;"
        )
        today = await con.fetchrow(
            """SELECT COALESCE(SUM(sell_price*qty),0) AS revenue,
                      COALESCE(SUM((sell_price-buy_price)*qty),0) AS profit,
                      COUNT(*) AS cnt
               FROM sales WHERE sold_at >= $1 AND sold_at < $2;""",
            start, end,
        )
        month = await con.fetchrow(
            """SELECT COALESCE(SUM(sell_price*qty),0) AS revenue,
                      COALESCE(SUM((sell_price-buy_price)*qty),0) AS profit,
                      COUNT(*) AS cnt
               FROM sales WHERE sold_at >= $1;""",
            month_start,
        )
        top = await con.fetch(
            """SELECT p.name,
                      SUM(s.qty) AS qty,
                      SUM(s.sell_price*s.qty) AS revenue,
                      SUM((s.sell_price-s.buy_price)*s.qty) AS profit
               FROM sales s JOIN products p ON p.id = s.product_id
               WHERE s.sold_at >= $1
               GROUP BY p.name ORDER BY revenue DESC LIMIT 15;""",
            month_start,
        )
        receipts30 = await con.fetch(
            """SELECT p.name, SUM(r.qty) AS qty
               FROM receipts r JOIN products p ON p.id = r.product_id
               WHERE r.received_at >= $1
               GROUP BY p.name ORDER BY qty DESC LIMIT 15;""",
            month_start,
        )
        shortages30 = await con.fetch(
            """SELECT p.name, SUM(c.diff) AS diff
               FROM inventory_checks c JOIN products p ON p.id = c.product_id
               WHERE c.checked_at >= $1 AND c.diff < 0
               GROUP BY p.name ORDER BY diff ASC LIMIT 15;""",
            month_start,
        )

    lines = ["=== ТОВАРИ НА СКЛАДІ ==="]
    for r in prods:
        low = " (ЗАКІНЧУЄТЬСЯ!)" if r["stock"] <= r["min_stock"] else ""
        lines.append(
            f"- {r['name']}: залишок {r['stock']} шт{low}, "
            f"закупка {float(r['buy_price']):.2f} грн, продаж {float(r['sell_price']):.2f} грн"
        )
    if not prods:
        lines.append("(товарів немає)")

    lines.append("\n=== СЬОГОДНІ ===")
    lines.append(
        f"Виторг: {float(today['revenue']):.2f} грн, прибуток: {float(today['profit']):.2f} грн, "
        f"продажів: {today['cnt']}"
    )
    lines.append("\n=== ЗА 30 ДНІВ ===")
    lines.append(
        f"Виторг: {float(month['revenue']):.2f} грн, прибуток: {float(month['profit']):.2f} грн, "
        f"продажів: {month['cnt']}"
    )
    lines.append("\n=== ТОП ТОВАРІВ ЗА 30 ДНІВ ===")
    for r in top:
        lines.append(
            f"- {r['name']}: продано {r['qty']} шт, виторг {float(r['revenue']):.2f} грн, "
            f"прибуток {float(r['profit']):.2f} грн"
        )
    if not top:
        lines.append("(продажів ще не було)")

    lines.append("\n=== ПРИХІД НА СКЛАД ЗА 30 ДНІВ ===")
    for r in receipts30:
        lines.append(f"- {r['name']}: прийнято {r['qty']} шт")
    if not receipts30:
        lines.append("(приходів не було)")

    lines.append("\n=== НЕДОСТАЧІ ЗА 30 ДНІВ (за звірками) ===")
    for r in shortages30:
        lines.append(f"- {r['name']}: недостача {abs(int(r['diff']))} шт")
    if not shortages30:
        lines.append("(недостач не виявлено)")

    return "\n".join(lines)


@app.post("/advisor")
async def advisor(a: AdvisorIn):
    """AI-советник: отвечает на вопросы владельца по данным магазина."""
    question = a.question.strip()
    if not question:
        raise HTTPException(400, "Питання не може бути порожнім")
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY не заданий на сервері")

    store_data = await collect_store_data()

    system = (
        "Ти — AI-порадник власника невеликого магазину в Україні. "
        "Відповідай українською, коротко і по суті (2-6 речень), з конкретними цифрами з даних. "
        "Якщо доречно — дай практичну пораду (що докупити, на що звернути увагу). "
        "Якщо в даних є недостачі — обов'язково зверни на них увагу власника. "
        "Не вигадуй дані, яких немає. Ось актуальні дані магазину:\n\n" + store_data
    )

    messages = []
    for m in a.history[-8:]:
        if m.get("role") in ("user", "assistant") and m.get("content"):
            messages.append({"role": m["role"], "content": str(m["content"])[:2000]})
    messages.append({"role": "user", "content": question[:2000]})

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-5",
                "max_tokens": 1000,
                "system": system,
                "messages": messages,
            },
        )
    if resp.status_code != 200:
        print("Anthropic error:", resp.status_code, resp.text[:300])
        raise HTTPException(502, "AI тимчасово недоступний, спробуй пізніше")

    answer = extract_text(resp.json())
    if not answer:
        raise HTTPException(502, "AI повернув порожню відповідь")
    return {"answer": answer}


# ------------------------------------------------------------
#  AI-ПРИЁМКА НАКЛАДНОЙ ПО ФОТО  [НОВОЕ]
#  Фото накладной -> Claude Vision -> позиции, сопоставленные
#  с товарами магазина -> владелец проверяет -> /receipts/bulk
# ------------------------------------------------------------
class ScanIn(BaseModel):
    image: str  # data:image/jpeg;base64,...


class BulkReceiptItem(BaseModel):
    product_id: int
    qty: int
    buy_price: Optional[float] = None


class BulkReceiptIn(BaseModel):
    items: List[BulkReceiptItem]


@app.post("/receipts/scan")
async def scan_invoice(s: ScanIn):
    """AI читает фото накладной и сопоставляет позиции с товарами магазина."""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY не заданий на сервері")
    img = (s.image or "").strip()
    if not img.startswith("data:image"):
        raise HTTPException(400, "Потрібне фото у форматі data:image")
    if len(img) > 4000000:
        raise HTTPException(400, "Фото завелике, спробуй ще раз")
    try:
        header, b64 = img.split(",", 1)
        media_type = header.split(":")[1].split(";")[0]
    except Exception:
        raise HTTPException(400, "Не вдалося прочитати фото")

    async with pool.acquire() as con:
        prods = await con.fetch("SELECT id, name FROM products ORDER BY name;")
    if not prods:
        raise HTTPException(400, "Спочатку додай товари в магазин")
    plist = "\n".join(str(r["id"]) + ": " + r["name"] for r in prods)
    name_map = {r["id"]: r["name"] for r in prods}

    prompt = (
        "Ти читаєш ФОТО НАКЛАДНОЇ від постачальника для невеликого магазину.\n\n"
        "Ось список товарів магазину (id: назва):\n" + plist + "\n\n"
        "Завдання:\n"
        "1. Розпізнай усі позиції накладної: назву, кількість (шт) і ціну закупки за ОДИНИЦЮ (грн).\n"
        "2. Зістав кожну позицію з товаром магазину ЗА ЗМІСТОМ (назви можуть відрізнятися: "
        "«Coca-Cola 0,5 ПЕТ» = «Кока-кола 0.5л»).\n"
        "3. Якщо кількість вказана ящиками/упаковками і видно фасування — переведи в штуки.\n"
        "4. Позиції, яких немає серед товарів магазину, поклади в unmatched.\n\n"
        "Відповідай СУВОРО валідним JSON без пояснень і без markdown:\n"
        "{\"items\": [{\"product_id\": 16, \"invoice_name\": \"Coca-Cola 0,5\", \"qty\": 24, \"buy_price\": 17.5}], "
        "\"unmatched\": [{\"invoice_name\": \"Спрайт 1л\", \"qty\": 12, \"buy_price\": 22}]}\n"
        "Якщо ціни за одиницю не видно — постав buy_price: null. Не вигадуй позиції, яких немає на фото."
    )

    payload = {
        "model": "claude-sonnet-5",
        "max_tokens": 2000,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
                {"type": "text", "text": prompt},
            ],
        }],
    }
    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
        )
    if resp.status_code != 200:
        print("Scan error:", resp.status_code, resp.text[:300])
        raise HTTPException(502, "AI тимчасово недоступний, спробуй пізніше")

    txt = extract_text(resp.json())
    if "```" in txt:
        txt = txt.replace("```json", "```").split("```")[1] if txt.count("```") >= 2 else txt
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        raise HTTPException(502, "AI не зміг розібрати накладну, спробуй чіткіше фото")
    try:
        parsed = json.loads(m.group(0))
    except Exception:
        raise HTTPException(502, "AI повернув некоректну відповідь, спробуй ще раз")

    items = []
    for it in (parsed.get("items") or []):
        try:
            pid = int(it.get("product_id"))
            qty = int(it.get("qty") or 0)
        except Exception:
            continue
        if pid not in name_map or qty <= 0:
            continue
        bp = it.get("buy_price")
        try:
            bp = round(float(bp), 2) if bp is not None else None
            if bp is not None and bp <= 0:
                bp = None
        except Exception:
            bp = None
        items.append({
            "product_id": pid,
            "name": name_map[pid],
            "invoice_name": str(it.get("invoice_name") or "")[:80],
            "qty": qty,
            "buy_price": bp,
        })

    unmatched = []
    for it in (parsed.get("unmatched") or []):
        unmatched.append({
            "invoice_name": str(it.get("invoice_name") or "?")[:80],
            "qty": it.get("qty"),
            "buy_price": it.get("buy_price"),
        })

    return {"ok": True, "items": items, "unmatched": unmatched}


@app.post("/receipts/bulk")
async def bulk_receipts(b: BulkReceiptIn):
    """Массовый приход: принимает сразу все позиции накладной."""
    if not b.items:
        raise HTTPException(400, "Список порожній")
    accepted = 0
    async with pool.acquire() as con:
        async with con.transaction():
            for it in b.items:
                if it.qty <= 0:
                    continue
                product = await con.fetchrow(
                    "SELECT * FROM products WHERE id=$1 FOR UPDATE;", it.product_id
                )
                if not product:
                    continue
                price = float(product["buy_price"])
                if it.buy_price is not None and it.buy_price > 0:
                    price = it.buy_price
                    await con.execute(
                        "UPDATE products SET buy_price=$1 WHERE id=$2;",
                        price, it.product_id,
                    )
                await con.execute(
                    "UPDATE products SET stock = stock + $1 WHERE id=$2;",
                    it.qty, it.product_id,
                )
                await con.execute(
                    """INSERT INTO receipts (product_id, qty, buy_price)
                       VALUES ($1, $2, $3);""",
                    it.product_id, it.qty, price,
                )
                accepted += 1
    return {"ok": True, "accepted": accepted}


# ------------------------------------------------------------
#  ИМПОРТ ТОВАРОВ: Excel / CSV / фото прайса  [НОВОЕ]
#  Онбординг магазина: кидаешь файл или фото -> AI превращает
#  в список товаров -> предпросмотр -> /products/bulk
# ------------------------------------------------------------
class ImportIn(BaseModel):
    rows: Optional[list] = None   # строки из Excel/CSV (массив массивов)
    image: Optional[str] = None   # или фото прайса (data:image...)


class NewProductIn(BaseModel):
    name: str
    buy_price: float = 0
    sell_price: float = 0
    stock: int = 0
    min_stock: int = 3


class BulkProductsIn(BaseModel):
    products: List[NewProductIn]


IMPORT_PROMPT = (
    "Ти перетворюєш дані на список товарів для системи обліку магазину в Україні.\n"
    "З наданих даних (таблиця з Excel/CSV або фото прайсу/зошита обліку) витягни ВСІ товари.\n"
    "Для кожного товару визнач:\n"
    "- name: назва товару\n"
    "- buy_price: закупівельна ціна за одиницю, грн\n"
    "- sell_price: ціна продажу за одиницю, грн\n"
    "- stock: залишок, шт\n"
    "Правила:\n"
    "- Якщо в даних лише одна ціна — став її у sell_price, а buy_price: 0.\n"
    "- Якщо залишок не вказано — став stock: 0.\n"
    "- Пропускай заголовки таблиці, підсумкові рядки, порожні рядки і все, що не є товаром.\n"
    "- Назви залишай як є, лише прибирай зайві пробіли. Не вигадуй товари.\n"
    "Відповідай СУВОРО валідним JSON без пояснень і без markdown:\n"
    "{\"products\": [{\"name\": \"Кока-кола 0.5л\", \"buy_price\": 17, \"sell_price\": 28, \"stock\": 24}]}"
)


@app.post("/products/import")
async def import_products(imp: ImportIn):
    """AI превращает Excel-строки или фото прайса в список товаров (предпросмотр)."""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY не заданий на сервері")

    content = []
    if imp.image:
        img = imp.image.strip()
        if not img.startswith("data:image"):
            raise HTTPException(400, "Потрібне фото у форматі data:image")
        if len(img) > 4000000:
            raise HTTPException(400, "Фото завелике")
        try:
            header, b64 = img.split(",", 1)
            media_type = header.split(":")[1].split(";")[0]
        except Exception:
            raise HTTPException(400, "Не вдалося прочитати фото")
        content.append({"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}})
        content.append({"type": "text", "text": IMPORT_PROMPT})
    elif imp.rows:
        lines = []
        for r in imp.rows[:300]:
            if not isinstance(r, list):
                continue
            cells = ["" if c is None else str(c)[:60] for c in r[:10]]
            line = " | ".join(cells)
            if line.replace("|", "").strip():
                lines.append(line)
        if not lines:
            raise HTTPException(400, "Файл виглядає порожнім")
        content.append({"type": "text", "text": IMPORT_PROMPT + "\n\nДАНІ ТАБЛИЦІ:\n" + "\n".join(lines)})
    else:
        raise HTTPException(400, "Надішли файл або фото")

    payload = {
        "model": "claude-sonnet-5",
        "max_tokens": 8000,
        "messages": [{"role": "user", "content": content}],
    }
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
        )
    if resp.status_code != 200:
        print("Import error:", resp.status_code, resp.text[:300])
        raise HTTPException(502, "AI тимчасово недоступний, спробуй пізніше")

    txt = extract_text(resp.json())
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        raise HTTPException(502, "AI не зміг розібрати дані, спробуй інший файл або чіткіше фото")
    try:
        parsed = json.loads(m.group(0))
    except Exception:
        raise HTTPException(502, "AI повернув некоректну відповідь, спробуй ще раз (можливо, файл завеликий — імпортуй частинами)")

    def to_price(x):
        try:
            v = round(float(x), 2)
            return v if v >= 0 else 0
        except Exception:
            return 0

    def to_int(x):
        try:
            v = int(float(x))
            return v if v >= 0 else 0
        except Exception:
            return 0

    out = []
    for it in (parsed.get("products") or [])[:250]:
        name = str(it.get("name") or "").strip()[:80]
        if not name:
            continue
        out.append({
            "name": name,
            "buy_price": to_price(it.get("buy_price")),
            "sell_price": to_price(it.get("sell_price")),
            "stock": to_int(it.get("stock")),
        })
    if not out:
        raise HTTPException(502, "Товарів у даних не знайдено")
    return {"ok": True, "products": out}


@app.post("/products/bulk")
async def bulk_products(b: BulkProductsIn):
    """Массовое создание товаров. Дубликаты по названию пропускаются."""
    if not b.products:
        raise HTTPException(400, "Список порожній")
    created = 0
    skipped = 0
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT name FROM products;")
        existing = {r["name"].strip().lower() for r in rows}
        for p in b.products[:250]:
            name = p.name.strip()
            if not name:
                continue
            if name.lower() in existing:
                skipped += 1
                continue
            await con.execute(
                """INSERT INTO products (name, buy_price, sell_price, stock, min_stock)
                   VALUES ($1, $2, $3, $4, $5);""",
                name[:80], max(p.buy_price, 0), max(p.sell_price, 0),
                max(p.stock, 0), max(p.min_stock, 1),
            )
            existing.add(name.lower())
            created += 1
    return {"ok": True, "created": created, "skipped": skipped}


# ------------------------------------------------------------
#  AI-ДИРЕКТОР: сам пишет владельцу в Telegram  [НОВОЕ]
#  - утренний AI-отчёт за вчера (раз в день, после 8:00 Киева)
#  - тревога "заканчивается товар" (проверка каждые 5 минут)
#  - тревога о недостаче уходит сразу из /inventory/check
#  Нужны переменные в Railway: TELEGRAM_BOT_TOKEN, OWNER_CHAT_ID
# ------------------------------------------------------------
async def tg_send(text: str):
    """Отправляет сообщение владельцу в Telegram. Молча пропускает, если не настроено."""
    if not TELEGRAM_BOT_TOKEN or not OWNER_CHAT_ID:
        return
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(
                "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage",
                json={"chat_id": OWNER_CHAT_ID, "text": text[:4000], "parse_mode": "HTML"},
            )
    except Exception as e:
        print("tg_send error:", str(e)[:200])


async def check_low_stock():
    """Тревога: товар опустился до минимума. Пишем один раз, пока не пополнят."""
    if not TELEGRAM_BOT_TOKEN or not OWNER_CHAT_ID:
        return
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT id, name, stock, min_stock FROM products
               WHERE stock <= min_stock AND COALESCE(low_notified, FALSE) = FALSE
               ORDER BY stock;"""
        )
        if rows:
            lines = "\n".join(
                "• " + r["name"] + ": залишилось " + str(r["stock"]) + " шт"
                + (" ❗️" if r["stock"] <= 0 else "")
                for r in rows[:15]
            )
            await tg_send("⚠️ <b>Закінчується товар:</b>\n" + lines +
                          "\n\n🚚 Час робити закупку!")
            await con.execute(
                "UPDATE products SET low_notified = TRUE WHERE id = ANY($1::int[]);",
                [r["id"] for r in rows],
            )
        # Пополнили — сбрасываем флаг, чтобы в следующий раз снова предупредить
        await con.execute(
            "UPDATE products SET low_notified = FALSE WHERE stock > min_stock AND low_notified = TRUE;"
        )


async def maybe_daily_report():
    """Утренний отчёт за вчера — один раз в день, после 8:00 по Киеву."""
    if not TELEGRAM_BOT_TOKEN or not OWNER_CHAT_ID:
        return
    now = datetime.now(KYIV)
    if now.hour < 8:
        return
    today_key = now.strftime("%Y-%m-%d")
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM app_state WHERE key = 'last_report';")
        if row and row["value"] == today_key:
            return
        await con.execute(
            """INSERT INTO app_state (key, value) VALUES ('last_report', $1)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;""",
            today_key,
        )
        # Данные за вчера
        y_start = (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).astimezone(timezone.utc)
        y_end = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        day = await con.fetchrow(
            """SELECT COALESCE(SUM(sell_price*qty),0) AS revenue,
                      COALESCE(SUM((sell_price-buy_price)*qty),0) AS profit,
                      COUNT(*) AS cnt
               FROM sales WHERE sold_at >= $1 AND sold_at < $2;""",
            y_start, y_end,
        )
        top = await con.fetch(
            """SELECT p.name, SUM(s.qty) AS qty, SUM(s.sell_price*s.qty) AS revenue
               FROM sales s JOIN products p ON p.id = s.product_id
               WHERE s.sold_at >= $1 AND s.sold_at < $2
               GROUP BY p.name ORDER BY revenue DESC LIMIT 5;""",
            y_start, y_end,
        )
        low = await con.fetch(
            "SELECT name, stock FROM products WHERE stock <= min_stock ORDER BY stock LIMIT 10;"
        )
        shorts = await con.fetch(
            """SELECT p.name, SUM(c.diff) AS diff
               FROM inventory_checks c JOIN products p ON p.id = c.product_id
               WHERE c.checked_at >= $1 AND c.diff < 0
               GROUP BY p.name LIMIT 10;""",
            y_start,
        )

    data_lines = ["ВЧОРА: виторг " + str(round(float(day["revenue"]), 2)) + " грн, "
                  "прибуток " + str(round(float(day["profit"]), 2)) + " грн, "
                  "продажів " + str(day["cnt"])]
    if top:
        data_lines.append("ТОП: " + "; ".join(
            r["name"] + " (" + str(r["qty"]) + " шт, " + str(round(float(r["revenue"]), 2)) + " грн)"
            for r in top))
    if low:
        data_lines.append("ЗАКІНЧУЄТЬСЯ: " + "; ".join(
            r["name"] + " (" + str(r["stock"]) + " шт)" for r in low))
    if shorts:
        data_lines.append("НЕДОСТАЧІ ЗА ДОБУ: " + "; ".join(
            r["name"] + " (" + str(abs(int(r["diff"]))) + " шт)" for r in shorts))
    data_text = "\n".join(data_lines)

    # Просим Claude оформить живой отчёт; если AI недоступен — шлём цифры как есть
    report = None
    if ANTHROPIC_API_KEY:
        try:
            payload = {
                "model": "claude-sonnet-5",
                "max_tokens": 500,
                "messages": [{"role": "user", "content":
                    "Ти — AI-директор невеликого магазину. Ось дані за вчора:\n\n" + data_text +
                    "\n\nНапиши власнику короткий ранковий звіт українською: 5-8 рядків, "
                    "з емодзі, по-дружньому але по суті. Обов'язково: цифри дня, топ товарів, "
                    "що докупити, і якщо є недостачі — зверни увагу. "
                    "Пиши звичайним текстом без markdown (без ** і #)."}],
            }
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=payload,
                )
            if resp.status_code == 200:
                report = extract_text(resp.json())
        except Exception as e:
            print("daily report AI error:", str(e)[:200])
    if not report:
        report = "☀️ Ранковий звіт\n" + data_text
    await tg_send("🤖 <b>AI-директор · ранковий звіт</b>\n\n" + report)


async def director_loop():
    """Фоновый цикл: каждые 5 минут проверяет остатки и время утреннего отчёта."""
    await asyncio.sleep(15)  # даём серверу спокойно стартовать
    print("AI-директор запущен" if (TELEGRAM_BOT_TOKEN and OWNER_CHAT_ID)
          else "AI-директор спит: не заданы TELEGRAM_BOT_TOKEN / OWNER_CHAT_ID")
    while True:
        try:
            if pool:
                await check_low_stock()
                await maybe_daily_report()
        except Exception as e:
            print("director_loop error:", str(e)[:200])
        await asyncio.sleep(300)


@app.get("/director/test")
async def director_test():
    """Проверка связи: открой в браузере — владельцу придёт тестовое сообщение."""
    if not TELEGRAM_BOT_TOKEN or not OWNER_CHAT_ID:
        return {"ok": False, "message": "Задай TELEGRAM_BOT_TOKEN і OWNER_CHAT_ID у Railway"}
    await tg_send("🤖 <b>AI-директор на зв'язку!</b>\nТепер я сам писатиму тобі: "
                  "ранковий звіт о 8:00, тривоги про залишки та недостачі.")
    return {"ok": True, "message": "Повідомлення надіслано! Перевір Telegram."}


# ------------------------------------------------------------
#  AI-ЗАКУПЩИК: что и сколько заказать  [НОВОЕ]
#  Считает скорость продаж за 14 дней и собирает список закупки
#  с запасом на 7 дней. AI добавляет короткий совет.
# ------------------------------------------------------------
async def build_purchase_plan():
    days_window = 14   # по какому периоду считаем скорость продаж
    cover_days = 7     # на сколько дней вперёд закупаемся
    since = (datetime.now(KYIV) - timedelta(days=days_window)).astimezone(timezone.utc)
    async with pool.acquire() as con:
        prods = await con.fetch("SELECT id, name, stock, min_stock FROM products ORDER BY name;")
        sold = await con.fetch(
            """SELECT product_id, COALESCE(SUM(qty),0) AS q
               FROM sales WHERE sold_at >= $1 GROUP BY product_id;""",
            since,
        )
    sold_map = {r["product_id"]: int(r["q"]) for r in sold}
    items = []
    for p in prods:
        s14 = sold_map.get(p["id"], 0)
        per_day = s14 / days_window
        stock = p["stock"]
        days_left = round(stock / per_day, 1) if per_day > 0 else None
        need = 0
        if per_day > 0:
            need = max(0, math.ceil(per_day * cover_days - stock))
        # Товар на пороге минимума — заказываем, даже если продажи редкие
        if stock <= p["min_stock"]:
            need = max(need, p["min_stock"] * 2 - stock, 1)
        if need > 0:
            items.append({
                "product_id": p["id"],
                "name": p["name"],
                "stock": stock,
                "per_day": round(per_day, 1),
                "days_left": days_left,
                "order_qty": int(need),
            })
    # Самое срочное — наверх (у кого меньше дней запаса)
    items.sort(key=lambda x: x["days_left"] if x["days_left"] is not None else 999)
    return items


@app.get("/purchase-plan")
async def purchase_plan():
    """Список закупки: что и сколько заказать, по скорости продаж."""
    items = await build_purchase_plan()
    advice = ""
    if ANTHROPIC_API_KEY and items:
        try:
            lines = "\n".join(
                f"- {it['name']}: залишок {it['stock']}, продається {it['per_day']}/день, "
                f"замовити {it['order_qty']} шт"
                for it in items[:20]
            )
            payload = {
                "model": "claude-sonnet-5",
                "max_tokens": 300,
                "messages": [{"role": "user", "content":
                    "Ти — AI-закупівельник магазину. Ось розрахований список закупівлі:\n" + lines +
                    "\n\nДай власнику 2-3 короткі поради українською щодо цієї закупівлі "
                    "(на що звернути увагу в першу чергу). Звичайний текст без markdown."}],
            }
            async with httpx.AsyncClient(timeout=40) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=payload,
                )
            if resp.status_code == 200:
                advice = extract_text(resp.json())
        except Exception as e:
            print("purchase advice error:", str(e)[:200])
    return {"ok": True, "items": items, "advice": advice}


@app.post("/purchase-plan/send")
async def purchase_plan_send():
    """Отправляет список закупки владельцу в Telegram."""
    if not TELEGRAM_BOT_TOKEN or not OWNER_CHAT_ID:
        raise HTTPException(400, "Задай TELEGRAM_BOT_TOKEN і OWNER_CHAT_ID у Railway")
    items = await build_purchase_plan()
    if not items:
        await tg_send("📝 <b>Список закупівлі</b>\n\nВсе в порядку — закуповувати поки нічого 👌")
        return {"ok": True, "count": 0}
    lines = "\n".join(
        "• " + it["name"] + " — <b>" + str(it["order_qty"]) + " шт</b>"
        + " (залишок " + str(it["stock"])
        + (", ~" + str(it["days_left"]) + " дн" if it["days_left"] is not None else "")
        + ")"
        for it in items[:30]
    )
    await tg_send("📝 <b>Список закупівлі на найближчі дні:</b>\n\n" + lines)
    return {"ok": True, "count": len(items)}


# ------------------------------------------------------------
#  КАРТОЧКА ТОВАРА (детальная статистика по одному товару)
# ------------------------------------------------------------
@app.get("/products/{product_id}/card")
async def product_card(product_id: int):
    """Всё об одном товаре: остаток, продажи за периоды, график за 30 дней."""
    start, end = today_bounds()
    month_start = (datetime.now(KYIV) - timedelta(days=29)).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(timezone.utc)
    year_start = (datetime.now(KYIV) - timedelta(days=364)).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(timezone.utc)

    async with pool.acquire() as con:
        p = await con.fetchrow("SELECT * FROM products WHERE id=$1;", product_id)
        if not p:
            raise HTTPException(404, "Товар не знайдено")

        async def agg(since):
            r = await con.fetchrow(
                """SELECT COALESCE(SUM(qty),0) AS qty,
                          COALESCE(SUM(sell_price*qty),0) AS revenue,
                          COALESCE(SUM((sell_price-buy_price)*qty),0) AS profit
                   FROM sales WHERE product_id=$1 AND sold_at >= $2;""",
                product_id, since,
            )
            return {
                "qty": int(r["qty"]),
                "revenue": float(r["revenue"]),
                "profit": float(r["profit"]),
            }

        today_s = await agg(start)
        month_s = await agg(month_start)
        year_s = await agg(year_start)

        days_rows = await con.fetch(
            """SELECT DATE(sold_at AT TIME ZONE 'Europe/Kyiv') AS day,
                      COALESCE(SUM(qty),0) AS qty,
                      COALESCE(SUM(sell_price*qty),0) AS revenue
               FROM sales
               WHERE product_id=$1 AND sold_at >= $2
               GROUP BY day ORDER BY day;""",
            product_id, month_start,
        )

    return {
        "id": p["id"],
        "name": p["name"],
        "buy_price": float(p["buy_price"]),
        "sell_price": float(p["sell_price"]),
        "stock": p["stock"],
        "min_stock": p["min_stock"],
        "img": p["img"],
        "barcode": p["barcode"],
        "low": p["stock"] <= p["min_stock"],
        "today": today_s,
        "month": month_s,
        "year": year_s,
        "days": [
            {
                "day": r["day"].strftime("%d.%m"),
                "qty": int(r["qty"]),
                "revenue": float(r["revenue"]),
            }
            for r in days_rows
        ],
    }
