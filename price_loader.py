import os
import requests
import xml.etree.ElementTree as ET
import psycopg2
import gzip
import io

# רשימת הכתובות המעודכנת
FEEDS = [
    {"chain": "Yohananof", "url": "https://publishedfiles.yohananof.co.il/PriceFull7290803800003-042-202602141300.gz"},
    {"chain": "Victory", "url": "https://matrixcatalog.co.il/NB_PublishPriceFull.aspx?id=1"}
]

def load_to_db(rows, chain):
    try:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            print("!!! ERROR: DATABASE_URL is missing in GitHub Secrets !!!")
            return
            
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # יצירת הטבלה אם היא לא קיימת למקרה שמשהו נמחק
        cur.execute("""
            CREATE TABLE IF NOT EXISTS store_prices (
                id SERIAL PRIMARY KEY,
                chain TEXT,
                item_code TEXT,
                price DECIMAL(10,2),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
        
        print(f"--- DATABASE: Inserting {len(rows)} items for {chain} ---")
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
        print(f"!!! SUCCESS: {chain} is in the Database !!!")
    except Exception as e:
        print(f"!!! DB ERROR: {e} !!!")

def download_and_extract(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
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
    # בדיקת דופק חובה - תמיד נכניס שורה אחת כדי לוודא שהצינור ל-Neon פתוח
    load_to_db([{"ItemCode": "999", "ItemPrice": "1.0"}], "SMART_MARKET_TEST")
    
    for feed in FEEDS:
        data = download_and_extract(feed['url'])
        if data:
            load_to_db(data, feed['chain'])
        else:
            print(f"Skipping {feed['chain']} due to download error.")
