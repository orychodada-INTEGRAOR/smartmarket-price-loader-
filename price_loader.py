import requests
import gzip
import csv
from io import BytesIO, TextIOWrapper
from datetime import datetime
import psycopg2
import os

PRICE_FEEDS = [
    {
        "chain": "shufersal",
        "url": "https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=1"
    },
    {
        "chain": "rami_levy",
        "url": "https://www.rami-levy.co.il/price/price.txt"
    }
]

def download_and_parse(feed):
    r = requests.get(feed["url"], timeout=60)
    gz_buffer = BytesIO(r.content)
    with gzip.GzipFile(fileobj=gz_buffer) as gz:
        text_stream = TextIOWrapper(gz, encoding='utf-8')
        reader = csv.DictReader(text_stream)
        return list(reader)

def load_to_db(rows, chain):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()

    for row in rows:
        barcode = row["ItemCode"]
        price = float(row["ItemPrice"])
        store_code = row["StoreId"]

        cur.execute("""
            INSERT INTO products (barcode, name)
            VALUES (%s, %s)
            ON CONFLICT (barcode) DO NOTHING
        """, (barcode, row["ItemName"]))

        cur.execute("""
            INSERT INTO store_prices (product_id, store_id, price, last_updated)
            SELECT p.id, s.id, %s, NOW()
            FROM products p, stores s
            WHERE p.barcode=%s AND s.store_code=%s
        """, (price, barcode, store_code))

    conn.commit()
    cur.close()
    conn.close()

for feed in PRICE_FEEDS:
    rows = download_and_parse(feed)
    load_to_db(rows, feed["chain"])
