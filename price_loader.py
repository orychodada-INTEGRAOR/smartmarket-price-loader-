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
        print(f"No rows found for {chain}")
        return
    
    # השורה הזו היא ה"פנס" שלנו - היא תדפיס ביומן את כל שמות העמודות
    print(f"DEBUG: Columns in {chain} are: {list(rows[0].keys())}")
    
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    
    for row in rows:
        # ניסיון למצוא את הקוד והמחיר בכל וריאציה אפשרית
        barcode = row.get("ItemCode") or row.get("itemcode") or row.get("Item_Code") or row.get("Item_code")
        price = row.get("ItemPrice") or row.get("itemprice") or row.get("Item_Price")
        
        if barcode and price:
            try:
                cur.execute(
                    "INSERT INTO store_prices (chain, item_code, price) VALUES (%s, %s, %s)",
                    (chain, barcode, float(price))
                )
            except Exception as e:
                continue # מדלג על שגיאות בודדות בתוך הלופ
                
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully loaded data for {chain}")
