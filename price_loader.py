import os
import requests
import xml.etree.ElementTree as ET
import psycopg2
import gzip
import io

# רשימת הכתובות המעודכנת ל-14.02 - SmartMarket Live Data
FEEDS = [
    {
        "chain": "Yohananof", 
        "url": "https://publishedfiles.yohananof.co.il/PriceFull7290803800003-042-202602141300.gz"
    },
    {
        "chain": "Victory", 
        "url": "https://matrixcatalog.co.il/NB_PublishPriceFull.aspx?id=1"
    }
]

def download_and_extract(url):
    try:
        print(f"Connecting to {url}...")
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            print(f"Failed to connect. Status: {r.status_code}")
            return None
        
        content = r.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        
        root = ET.fromstring(content)
        items = root.findall(".//Item") or root.findall(".//Product")
        print(f"Found {len(items)} items in XML.")
        return [{child.tag: child.text for child in item} for item in items]
    except Exception as e:
        print(f"Error: {e}")
        return None

def load_to_db(rows, chain):
    if not rows: return
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
    
    print(f"Inserting data for {chain} into Neon...")
    for row in rows:
        item_code = row.get("ItemCode") or row.get("itemcode")
        price = row.get("ItemPrice") or row.get("itemprice")
        if item_code and price:
            try:
                cur.execute(
                    "INSERT INTO store_prices (chain, item_code, price) VALUES (%s, %s, %s)",
                    (chain, item_code, float(price))
                )
            except: continue
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    for feed in FEEDS:
        data = download_and_extract(feed['url'])
        if data:
            load_to_db(data, feed['chain'])
            print(f"Finished {feed['chain']}")
