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
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
        
        print(f"--- DATABASE: Inserting {len(rows)} items for {chain} ---")
        for row in rows:
            item_code = row.get("ItemCode") or row.get("itemcode")
            price = row.get("ItemPrice") or row.get("itemprice")
            if item_code and price:
                cur.execute(
                    "INSERT INTO store_prices (chain, item_code, price) VALUES (%s, %s, %s)",
                    (chain, item_code, float(price))
                )
        conn.commit()
        cur.close()
        conn.close()
        print(f"!!! SUCCESS: {chain} is in the Database !!!")
    except Exception as e:
        print(f"!!! DB ERROR: {e} !!!")

def download_and_extract(url):
    try:
        # הוספת Headers כדי שלא יחסמו אותנו
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, timeout=30, headers=headers)
        r.raise_for_status()
        content = r.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        root = ET.fromstring(content)
        items = root.findall(".//Item") or root.findall(".//Product")
        return [{child.tag: child.text for child in item} for item in items]
    except Exception as e:
        print(f"Download Error for {url}: {e}")
        return None

if __name__ == "__main__":
    # בדיקת דופק לצינור - תמיד נכניס פריט אחד לפחות כדי לראות שה-DB עובד
    load_to_db([{"ItemCode": "000", "ItemPrice": "0.0"}], "TEST_CONNECTION")
    
    for feed in FEEDS:
        data = download_and_extract(feed['url'])
        if data:
            load_to_db(data, feed['chain'])['chain'])
