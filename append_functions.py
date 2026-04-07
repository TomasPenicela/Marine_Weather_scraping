
def insert_ctd(csv_content, db_path="weather.db"):
    import sqlite3
    import csv
    import io
    """Insere dados de CTD (Condutividade/Temperatura/Profundidade)"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)
    next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 3:
            continue
        try:
            def safe_int(val):
                if not val or val.strip() == '':
                    return None
                try:
                    val = str(val).replace('%', '').strip()
                    return int(float(val))
                except:
                    return None
            cursor.execute('INSERT OR IGNORE INTO ctd (site_id, site_name, date_time, quality_percent) VALUES (?, ?, ?, ?)', 
                          (int(row[0]), row[1], row[2], safe_int(row[3]) if len(row) > 3 else None))
            inserted += 1
        except:
            pass
    conn.commit()
    conn.close()
    return inserted

def insert_air_quality(csv_content, db_path="weather.db"):
    import sqlite3
    import csv
    import io
    """Insere dados de qualidade do ar"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)
    next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 3:
            continue
        try:
            def safe_int(val):
                if not val or val.strip() == '':
                    return None
                try:
                    val = str(val).replace('%', '').strip()
                    return int(float(val))
                except:
                    return None
            cursor.execute('INSERT OR IGNORE INTO air_quality (site_id, site_name, date_time, quality_percent) VALUES (?, ?, ?, ?)', 
                          (int(row[0]), row[1], row[2], safe_int(row[3]) if len(row) > 3 else None))
            inserted += 1
        except:
            pass
    conn.commit()
    conn.close()
    return inserted

def insert_currents(csv_content, db_path="weather.db"):
    import sqlite3
    import csv
    import io
    """Insere dados de correntes"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)
    next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 3:
            continue
        try:
            def safe_int(val):
                if not val or val.strip() == '':
                    return None
                try:
                    val = str(val).replace('%', '').strip()
                    return int(float(val))
                except:
                    return None
            cursor.execute('INSERT OR IGNORE INTO currents (site_id, site_name, date_time, quality_percent) VALUES (?, ?, ?, ?)', 
                          (int(row[0]), row[1], row[2], safe_int(row[3]) if len(row) > 3 else None))
            inserted += 1
        except:
            pass
    conn.commit()
    conn.close()
    return inserted
