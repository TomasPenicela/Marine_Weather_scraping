import sqlite3
import csv
import io
from datetime import datetime

def create_tables(db_path="weather.db"):
    """Cria tabelas bem estruturadas para dados de 2025"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # ===== TIDES (Marés - Dataset 1) =====
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            site_name TEXT,
            date_time DATETIME,
            observed_m REAL,
            predicted_m REAL,
            surge_m REAL,
            msl_m REAL,
            residual_m REAL,
            stddev REAL,
            status TEXT,
            quality_percent INTEGER,
            quality_flag INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')
    
    # ===== CTD - CONDUCTIVITY/TEMPERATURE/DEPTH (Dataset 2) =====
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ctd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            site_name TEXT,
            date_time DATETIME,
            conductivity REAL,
            temperature_celsius REAL,
            depth_m REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')
    
    # ===== WQ - WATER QUALITY (Dataset 3) =====
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS water_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            site_name TEXT,
            date_time DATETIME,
            temperature_celsius REAL,
            quality_percent INTEGER,
            records_count INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')
    
    # ===== MET - METEOROLOGICAL (Dataset 5) =====
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS meteorological (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            site_name TEXT,
            date_time DATETIME,
            elevation_m REAL,
            wind_speed_ms REAL,
            wind_direction_deg REAL,
            wind_u_ms REAL,
            wind_v_ms REAL,
            gust_speed_ms REAL,
            gust_direction_deg REAL,
            gust_u_ms REAL,
            gust_v_ms REAL,
            atmos_pressure_mbar REAL,
            temperature_celsius REAL,
            humidity_percent REAL,
            dew_point_celsius REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')
    
    # ===== WAVES (Ondas - Dataset 6) =====
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS waves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            site_name TEXT,
            date_time DATETIME,
            wave_height_sig_m REAL,
            wave_height_max_m REAL,
            wave_period_peak_s REAL,
            zero_upcross_period_mean_s REAL,
            wave_period_sig_s REAL,
            wave_period_mean_s REAL,
            wave_direction_mean_deg REAL,
            wave_direction_peak_deg REAL,
            directional_spread_mean_deg REAL,
            total_energy REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')
    
    # ===== AQ - AIR QUALITY (Dataset 4) =====
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS air_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            site_name TEXT,
            date_time DATETIME,
            pollutant_level REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')
    
    # ===== CURRENTS (Correntes - Dataset 7) =====
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS currents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            site_name TEXT,
            date_time DATETIME,
            current_speed_ms REAL,
            current_direction_deg REAL,
            depth_m REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')
    
    # Criar índices para melhor performance
    indices = [
        ("tides", "date_time"),
        ("ctd", "date_time"),
        ("water_quality", "date_time"),
        ("air_quality", "date_time"),
        ("meteorological", "date_time"),
        ("waves", "date_time"),
        ("currents", "date_time"),
    ]
    
    for table, column in indices:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column})")
        except:
            pass
    
    conn.commit()
    conn.close()

def safe_float(val):
    """Converte valor para float de forma segura"""
    if not val or val.strip() == '':
        return None
    try:
        return float(val)
    except:
        return None

def safe_int(val):
    """Converte valor para int de forma segura"""
    if not val or val.strip() == '':
        return None
    try:
        # Remover % se existir
        val = str(val).replace('%', '').strip()
        return int(float(val))
    except:
        return None

def insert_tides(csv_content, db_path="weather.db"):
    """Insere dados de marés"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)  # Pula header
    next(reader)  # Pula unidades
    
    inserted = 0
    for row in reader:
        if not row or len(row) < 12:
            continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO tides 
                (site_id, site_name, date_time, observed_m, predicted_m, surge_m,
                 msl_m, residual_m, stddev, status, quality_percent, quality_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]),
                row[1],
                row[2],
                safe_float(row[3]),
                safe_float(row[4]),
                safe_float(row[5]),
                safe_float(row[6]),
                safe_float(row[7]),
                safe_float(row[8]),
                row[9],
                safe_int(row[10]),
                safe_int(row[11])
            ))
            inserted += 1
        except Exception as e:
            pass
    
    conn.commit()
    conn.close()
    return inserted

def insert_water_quality(csv_content, db_path="weather.db"):
    """Insere dados de qualidade de água"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    reader = csv.reader(io.StringIO(csv_content))
    next(reader)  # Pula header
    next(reader)  # Pula unidades
    
    inserted = 0
    for row in reader:
        if not row or len(row) < 6:
            continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO water_quality
                (site_id, site_name, date_time, temperature_celsius, quality_percent, records_count)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]),
                row[1],
                row[2],
                safe_float(row[3]),
                safe_int(row[4]),
                safe_int(row[5])
            ))
            inserted += 1
        except Exception as e:
            pass
    
    conn.commit()
    conn.close()
    return inserted

def insert_meteorological(csv_content, db_path="weather.db"):
    """Insere dados meteorológicos"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)  # Pula header
    next(reader)  # Pula unidades
    
    inserted = 0
    for row in reader:
        if not row or len(row) < 17:
            continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO meteorological
                (site_id, site_name, date_time, elevation_m, wind_speed_ms, wind_direction_deg,
                 wind_u_ms, wind_v_ms, gust_speed_ms, gust_direction_deg, gust_u_ms, gust_v_ms,
                 atmos_pressure_mbar, temperature_celsius, humidity_percent, dew_point_celsius, quality_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]),
                row[1],
                row[2],
                safe_float(row[3]),
                safe_float(row[4]),
                safe_float(row[5]),
                safe_float(row[6]),
                safe_float(row[7]),
                safe_float(row[8]),
                safe_float(row[9]),
                safe_float(row[10]),
                safe_float(row[11]),
                safe_float(row[12]),
                safe_float(row[13]),
                safe_float(row[14]),
                safe_float(row[15]),
                safe_int(row[16])
            ))
            inserted += 1
        except Exception as e:
            pass
    
    conn.commit()
    conn.close()
    return inserted

def insert_waves(csv_content, db_path="weather.db"):
    """Insere dados de ondas"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)  # Pula header
    next(reader)  # Pula unidades
    
    inserted = 0
    for row in reader:
        if not row or len(row) < 14:
            continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO waves
                (site_id, site_name, date_time, wave_height_sig_m, wave_height_max_m,
                 wave_period_peak_s, zero_upcross_period_mean_s, wave_period_sig_s, wave_period_mean_s,
                 wave_direction_mean_deg, wave_direction_peak_deg, directional_spread_mean_deg,
                 total_energy, quality_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]),
                row[1],
                row[2],
                safe_float(row[3]),
                safe_float(row[4]),
                safe_float(row[5]),
                safe_float(row[6]),
                safe_float(row[7]),
                safe_float(row[8]),
                safe_float(row[9]),
                safe_float(row[10]),
                safe_float(row[11]),
                safe_float(row[12]),
                safe_int(row[13])
            ))
            inserted += 1
        except Exception as e:
            pass
    
    conn.commit()
    conn.close()
    return inserted

def insert_ctd(csv_content, db_path="weather.db"):
    """Insere dados de CTD"""
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
            cursor.execute("INSERT OR IGNORE INTO ctd (site_id, site_name, date_time, quality_percent) VALUES (?, ?, ?, ?)", 
                          (int(row[0]), row[1], row[2], safe_int(row[3]) if len(row) > 3 else None))
            inserted += 1
        except:
            pass
    conn.commit()
    conn.close()
    return inserted

def insert_air_quality(csv_content, db_path="weather.db"):
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
            cursor.execute("INSERT OR IGNORE INTO air_quality (site_id, site_name, date_time, quality_percent) VALUES (?, ?, ?, ?)", 
                          (int(row[0]), row[1], row[2], safe_int(row[3]) if len(row) > 3 else None))
            inserted += 1
        except:
            pass
    conn.commit()
    conn.close()
    return inserted

def insert_currents(csv_content, db_path="weather.db"):
    """Insere dados de correntes  
    
    CSV esperado: Site ID, Site Name, Date Time, Current Speed, Current Direction, Depth, Quality Percent
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)  # Pula header
    
    inserted = 0
    for row in reader:
        if not row or len(row) < 3:
            continue
        try:
            # Extrair valores com índices corretos
            site_id = int(row[0])
            site_name = row[1]
            date_time = row[2]
            current_speed_ms = safe_float(row[3]) if len(row) > 3 else None
            current_direction_deg = safe_float(row[4]) if len(row) > 4 else None
            depth_m = safe_float(row[5]) if len(row) > 5 else None
            quality_percent = safe_int(row[6]) if len(row) > 6 else None
            
            cursor.execute('''
                INSERT OR IGNORE INTO currents 
                (site_id, site_name, date_time, current_speed_ms, current_direction_deg, depth_m, quality_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (site_id, site_name, date_time, current_speed_ms, current_direction_deg, depth_m, quality_percent))
            inserted += 1
        except Exception as e:
            pass
    conn.commit()
    conn.close()
    return inserted
