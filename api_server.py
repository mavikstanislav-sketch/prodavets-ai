# ============================================================
#  Продавець AI — api_server.py (версия 1: фундамент)
#  FastAPI + PostgreSQL (Railway)
#  Что умеет:
#   - товары: добавить / список / изменить / удалить
#   - продажи: пробить продажу (списывает остаток)
#   - статистика: за сегодня и за период
#   - лента последних продаж (для живого дашборда)
# ============================================================

import os
from datetime import datetime, timedelta, timezone

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

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
    print("База готова: таблицы products и sales на месте")


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


class SaleIn(BaseModel):
    product_id: int
    qty: int = 1


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
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """INSERT INTO products (name, buy_price, sell_price, stock, min_stock)
               VALUES ($1, $2, $3, $4, $5) RETURNING id;""",
            name, p.buy_price, p.sell_price, p.stock, p.min_stock,
        )
    return {"ok": True, "id": row["id"]}


@app.put("/products/{product_id}")
async def update_product(product_id: int, p: ProductIn):
    """Изменить товар (название, цены, остаток, порог)."""
    async with pool.acquire() as con:
        result = await con.execute(
            """UPDATE products
               SET name=$1, buy_price=$2, sell_price=$3, stock=$4, min_stock=$5
               WHERE id=$6;""",
            p.name.strip(), p.buy_price, p.sell_price, p.stock, p.min_stock,
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
