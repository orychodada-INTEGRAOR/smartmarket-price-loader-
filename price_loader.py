import os
import requests
import xml.etree.ElementTree as ET
import psycopg2
import gzip
import io

# רשימת כל הרשתות - SmartMarket Coverage
FEEDS = [
 FEEDS = [
    {
        "chain": "Yohananof", 
        "url": "https://publishedfiles.yohananof.co.il/PriceFull7290803800003-042-202602140700.gz"
    }
]
def download_and_extract(url):
    try:
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            return None
        
        content = response.content
        # בדיקה אם הקובץ דחוס (GZIP) - תיקון רווחים כאן
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        
        root = ET.fromstring(content)
        items = root.findall(".//Item") or root.findall(".//Product")
        
        return [{child.tag: child.text for child in item} for item in items]
    except Exception as e:
        print(f"Error with {url}: {e}")
        return None

def load_to_db(rows, chain):
    if not rows: return
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
    for row in rows:
        barcode = row.get("ItemCode") or row.get("itemcode")
        price = row.get("ItemPrice") or row.get("itemprice")
        if barcode and price:
            try:
                cur.execute(
                    "INSERT INTO store_prices (chain, item_code, price) VALUES (%s, %s, %s)",
                    (chain, barcode, float(price))
                )
            except: continue
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} items for {chain}")

# הרצה ראשית
if __name__ == "__main__":
    for feed in FEEDS:
        print(f"Processing {feed['chain']}...")
        data = download_and_extract(feed['url'])
        if data:
            load_to_db(data, feed['chain'])
