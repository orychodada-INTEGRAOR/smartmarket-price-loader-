import os
import requests
import xml.etree.ElementTree as ET
import psycopg2
import gzip
import io

# רשימת ה-Feeds המעודכנת לכל הרשתות המרכזיות
FEEDS = [
    {"chain": "Yohananof", "url": "https://publishedfiles.yohananof.co.il/PriceFull7290803800003-042-202602141300.gz"},
    {"chain": "Victory", "url": "https://matrixcatalog.co.il/NB_PublishPriceFull.aspx?id=1"},
    {"chain": "RamiLevy", "url": "https://url.retail.prices.pali.co.il/FileObject/UpdateCategory?catID=2&warehouseId=341"},
    {"chain": "Shufersal", "url": "https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2&warehouseId=336"},
    {"chain": "StopMarket", "url": "https://url.retail.prices.pali.co.il/FileObject/UpdateCategory?catID=2&warehouseId=1"}
]

def load_to_db(rows, chain):
    try:
        db_url = os.getenv("DATABASE_URL")
        if not db_url: return
            
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # וידוא קיום טבלה
        cur.execute("""
            CREATE TABLE IF NOT EXISTS store_prices (
                id SERIAL PRIMARY KEY,
                chain TEXT,
                item_code TEXT,
                price DECIMAL(10,2),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ניקוי נתונים ישנים של אותה רשת לפני טעינה חדשה
        cur.execute("DELETE FROM store_prices WHERE chain = %s", (chain,))
        
        print(f"--- DB: Inserting {len(rows)} items for {chain} ---")
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
        print(f"!!! SUCCESS: {chain} Sync Complete !!!")
    except Exception as e:
        print(f"!!! DB ERROR for {chain}: {e} !!!")

def download_and_extract(url):
    try:
        # התחזות לדפדפן כדי למנוע חסימות
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        r = requests.get(url, timeout=45, headers=headers)
        r.raise_for_status()
        
        content = r.content
        # בדיקה אם הקובץ דחוס (GZIP)
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
                
        root = ET.fromstring(content)
        items = root.findall(".//Item") or root.findall(".//Product")
        return [{child.tag: child.text for child in item} for item in items]
    except Exception as e:
        print(f"Skipping {url}: {e}")
        return None

if __name__ == "__main__":
    # בדיקת דופק מערכתית
    load_to_db([{"ItemCode": "777", "ItemPrice": "7.77"}], "SYSTEM_CHECK")
    
    for feed in FEEDS:
        data = download_and_extract(feed['url'])
        if data:
            load_to_db(data, feed['chain'])
        else:
            print(f"Network error or block on {feed['chain']}. Moving to next.")
