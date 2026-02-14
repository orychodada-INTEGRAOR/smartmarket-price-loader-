import os
import requests
import xml.etree.ElementTree as ET
import psycopg2
import gzip
import io

# רשימת כתובות - SmartMarket Data Source
FEEDS = [
    {
        "chain": "Yohananof", 
        "url": "https://publishedfiles.yohananof.co.il/PriceFull7290803800003-042-202602140700.gz"
    }
]

def download_and_extract(url):
    try:
        print(f"Downloading from {url}...")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        
        content = r.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        
        root = ET.fromstring(content)
        items = root.findall(".//Item") or root.findall(".//Product")
        return [{child.tag: child.text for child in item} for item in items]
    except Exception as e:
        print(f"Error downloading: {e}")
        return None

def load_to_db(rows, chain):
    if not rows:
        print(f"No rows found for {chain}")
        return
    
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    
    # ניקוי נתונים ישנים של אותה רשת
    cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
    
    print(f"Loading {len(rows)} items into Neon...")
    for row in rows:
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
    print(f"Successfully loaded {chain}")

if __name__ == "__main__":
    for feed in FEEDS:
        data = download_and_extract(feed['url'])
        if data:
            load_to_db(data, feed['chain'])
        else:
            print(f"Failed to get data for {feed['chain']}")
