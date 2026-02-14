import os
import requests
import xml.etree.ElementTree as ET
import psycopg2
import gzip
import io

FEEDS = [
    {"chain": "Yohananof", "url": "https://publishedfiles.yohananof.co.il/PriceFull7290803800003-042-202602141300.gz"},
    {"chain": "Victory", "url": "https://matrixcatalog.co.il/NB_PublishPriceFull.aspx?id=1"}
]

def load_to_db(rows, chain):
    try:
        url = os.getenv("DATABASE_URL")
        print(f"Connecting to DB... (URL starts with: {url[:20]}...)")
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        
        print(f"Deleting old data for {chain}...")
        cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
        
        print(f"Inserting {len(rows)} items...")
        for row in rows:
            item_code = row.get("ItemCode") or row.get("itemcode")
            price = row.get("ItemPrice") or row.get("itemprice")
            if item_code and price:
                cur.execute(
                    "INSERT INTO store_prices (chain, item_code, price) VALUES (%s, %s, %s)",
                    (chain, item_code, float(price))
                )
        
        conn.commit()
        print(f"!!! SUCCESS: {chain} LOADED !!!")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"!!! DATABASE ERROR: {e} !!!")

def download_and_extract(url):
    try:
        r = requests.get(url, timeout=60)
        content = r.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        root = ET.fromstring(content)
        items = root.findall(".//Item") or root.findall(".//Product")
        return [{child.tag: child.text for child in item} for item in items]
    except Exception as e:
        print(f"Download Error: {e}")
        return None

if __name__ == "__main__":
    for feed in FEEDS:
        data = download_and_extract(feed['url'])
        if data:
            load_to_db(data, feed['chain'])
