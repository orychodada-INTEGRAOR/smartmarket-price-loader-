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
FEEDS = [
    {
        "chain": "Shufersal", 
        "url": "http://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2&warehouseId=123"
    },
    {
        "chain": "Rami_Levy", 
        "url": "https://url-to-rami-levy-prices.co.il/PriceFull-current.xml"
    },
    {
        "chain": "Yohananof", 
        "url": "https://publishedfiles.yohananof.co.il/PriceFull7290803800003-042-202602140700.gz"
    }
]

def download_and_extract(url):
    print(f"Downloading from {feed['chain']}...")
    r = requests.get(feed["url"], timeout=60)
    r.raise_for_status() # מוודא שההורדה הצליחה
    
    # בדיקה: האם הקובץ מכווץ ב-GZIP?
    if r.content.startswith(b'\x1f\x8b'):
        gz_buffer = BytesIO(r.content)
        with gzip.GzipFile(fileobj=gz_buffer) as gz:
            text_stream = TextIOWrapper(gz, encoding='utf-8')
            return list(csv.DictReader(text_stream))
    else:
        # אם זה טקסט רגיל
        text_content = r.content.decode('utf-8')
        return list(csv.DictReader(text_content.splitlines()))

def load_to_db(rows, chain):
    if not rows:
        return
    
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    
    # ניקוי נתונים ישנים של אותה רשת לפני טעינה חדשה (אופציונלי)
    cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
    
    for row in rows:
        # שליפת הנתונים לפי השמות המדויקים משופרסל/רמי לוי
        item_code = row.get("ItemCode") or row.get("itemcode")
        price = row.get("ItemPrice") or row.get("itemprice")
        
        if item_code and price:
            try:
                cur.execute(
                    "INSERT INTO store_prices (chain, item_code, price) VALUES (%s, %s, %s)",
                    (chain, item_code, float(price))
                )
            except:
                continue
                
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully loaded {len(rows)} items for {chain}")
