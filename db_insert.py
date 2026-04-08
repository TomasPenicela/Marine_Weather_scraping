"""
🌊 WEATHER DATA DATABASE INSERT MODULE
Módulo responsável pela criação de tabelas e inserção de dados no SQLite
"""

import sqlite3
import csv
import io

try:
    import pyodbc
except ImportError:
    pyodbc = None

DB_SERVER = r"KENMOZ-DB04\PRODUCTION"
DB_NAME = "KMR_OPSDATA"
DB_DRIVER = "{ODBC Driver 17 for SQL Server}"

SQL_SERVER_TABLES = {
    "tides": [
        "site_id", "site_name", "date_time", "observed_m", "predicted_m", "surge_m",
        "msl_m", "residual_m", "stddev", "status", "quality_percent", "quality_flag"
    ],
    "ctd": ["site_id", "site_name", "date_time", "conductivity", "temperature_celsius", "depth_m", "quality_percent"],
    "water_quality": ["site_id", "site_name", "date_time", "temperature_celsius", "quality_percent", "records_count"],
    "meteorological": [
        "site_id", "site_name", "date_time", "elevation_m", "wind_speed_ms", "wind_direction_deg",
        "wind_u_ms", "wind_v_ms", "gust_speed_ms", "gust_direction_deg", "gust_u_ms", "gust_v_ms",
        "atmos_pressure_mbar", "temperature_celsius", "humidity_percent", "dew_point_celsius", "quality_percent"
    ],
    "waves": [
        "site_id", "site_name", "date_time", "wave_height_sig_m", "wave_height_max_m",
        "wave_period_peak_s", "zero_upcross_period_mean_s", "wave_period_sig_s", "wave_period_mean_s",
        "wave_direction_mean_deg", "wave_direction_peak_deg", "directional_spread_mean_deg", "total_energy", "quality_percent"
    ],
    "air_quality": ["site_id", "site_name", "date_time", "pollutant_level", "quality_percent"],
    "currents": ["site_id", "site_name", "date_time", "current_speed_ms", "current_direction_deg", "depth_m", "quality_percent"]
}

SQL_SERVER_CREATE_QUERIES = {
    "tides": '''
        IF OBJECT_ID('dbo.tides', 'U') IS NULL
        CREATE TABLE dbo.tides (
            id INT IDENTITY(1,1) PRIMARY KEY,
            site_id INT,
            site_name NVARCHAR(255),
            date_time DATETIME2,
            observed_m FLOAT,
            predicted_m FLOAT,
            surge_m FLOAT,
            msl_m FLOAT,
            residual_m FLOAT,
            stddev FLOAT,
            status NVARCHAR(100),
            quality_percent INT,
            quality_flag INT,
            created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_tides_site_date UNIQUE(site_id, date_time)
        )''',
    "ctd": '''
        IF OBJECT_ID('dbo.ctd', 'U') IS NULL
        CREATE TABLE dbo.ctd (
            id INT IDENTITY(1,1) PRIMARY KEY,
            site_id INT,
            site_name NVARCHAR(255),
            date_time DATETIME2,
            conductivity FLOAT,
            temperature_celsius FLOAT,
            depth_m FLOAT,
            quality_percent INT,
            created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_ctd_site_date UNIQUE(site_id, date_time)
        )''',
    "water_quality": '''
        IF OBJECT_ID('dbo.water_quality', 'U') IS NULL
        CREATE TABLE dbo.water_quality (
            id INT IDENTITY(1,1) PRIMARY KEY,
            site_id INT,
            site_name NVARCHAR(255),
            date_time DATETIME2,
            temperature_celsius FLOAT,
            quality_percent INT,
            records_count INT,
            created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_water_quality_site_date UNIQUE(site_id, date_time)
        )''',
    "meteorological": '''
        IF OBJECT_ID('dbo.meteorological', 'U') IS NULL
        CREATE TABLE dbo.meteorological (
            id INT IDENTITY(1,1) PRIMARY KEY,
            site_id INT,
            site_name NVARCHAR(255),
            date_time DATETIME2,
            elevation_m FLOAT,
            wind_speed_ms FLOAT,
            wind_direction_deg FLOAT,
            wind_u_ms FLOAT,
            wind_v_ms FLOAT,
            gust_speed_ms FLOAT,
            gust_direction_deg FLOAT,
            gust_u_ms FLOAT,
            gust_v_ms FLOAT,
            atmos_pressure_mbar FLOAT,
            temperature_celsius FLOAT,
            humidity_percent FLOAT,
            dew_point_celsius FLOAT,
            quality_percent INT,
            created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_meteorological_site_date UNIQUE(site_id, date_time)
        )''',
    "waves": '''
        IF OBJECT_ID('dbo.waves', 'U') IS NULL
        CREATE TABLE dbo.waves (
            id INT IDENTITY(1,1) PRIMARY KEY,
            site_id INT,
            site_name NVARCHAR(255),
            date_time DATETIME2,
            wave_height_sig_m FLOAT,
            wave_height_max_m FLOAT,
            wave_period_peak_s FLOAT,
            zero_upcross_period_mean_s FLOAT,
            wave_period_sig_s FLOAT,
            wave_period_mean_s FLOAT,
            wave_direction_mean_deg FLOAT,
            wave_direction_peak_deg FLOAT,
            directional_spread_mean_deg FLOAT,
            total_energy FLOAT,
            quality_percent INT,
            created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_waves_site_date UNIQUE(site_id, date_time)
        )''',
    "air_quality": '''
        IF OBJECT_ID('dbo.air_quality', 'U') IS NULL
        CREATE TABLE dbo.air_quality (
            id INT IDENTITY(1,1) PRIMARY KEY,
            site_id INT,
            site_name NVARCHAR(255),
            date_time DATETIME2,
            pollutant_level FLOAT,
            quality_percent INT,
            created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_air_quality_site_date UNIQUE(site_id, date_time)
        )''',
    "currents": '''
        IF OBJECT_ID('dbo.currents', 'U') IS NULL
        CREATE TABLE dbo.currents (
            id INT IDENTITY(1,1) PRIMARY KEY,
            site_id INT,
            site_name NVARCHAR(255),
            date_time DATETIME2,
            current_speed_ms FLOAT,
            current_direction_deg FLOAT,
            depth_m FLOAT,
            quality_percent INT,
            created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_currents_site_date UNIQUE(site_id, date_time)
        )'''
}


def get_sql_type(col):
    if col in ['site_id', 'quality_percent', 'quality_flag', 'records_count']:
        return 'INT'
    elif col == 'site_name':
        return 'NVARCHAR(255)'
    elif col == 'date_time':
        return 'DATETIME2'
    elif col == 'status':
        return 'NVARCHAR(100)'
    else:
        return 'FLOAT'


def get_sql_server_connection():
    if pyodbc is None:
        raise ImportError("pyodbc is necessário para conexão com SQL Server")

    conn_str = (
        f"DRIVER={DB_DRIVER};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )
    print(f"[db_insert] Conectando ao SQL Server: {DB_SERVER} / {DB_NAME}")
    return pyodbc.connect(conn_str, autocommit=True)


def ensure_sql_server_tables():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    print("[db_insert] Garantindo criação das tabelas SQL Server...")
    for table_name, query in SQL_SERVER_CREATE_QUERIES.items():
        print(f"[db_insert] Criando/verificando tabela: {table_name}")
        try:
            cursor.execute(query)
        except Exception as exc:
            print(f"[db_insert] Erro criando tabela {table_name}: {exc}")

    cursor.close()
    conn.close()


def get_sql_server_max_date(table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT MAX(date_time) FROM dbo.{table_name}")
        result = cursor.fetchone()[0]
        return result
    finally:
        cursor.close()
        conn.close()


def sync_table_to_sql_server(table_name, sqlite_db_path="weather.db", batch_size=1000):
    if pyodbc is None:
        raise ImportError("pyodbc é necessário para sincronizar com SQL Server")

    ensure_sql_server_tables()
    sql_conn = get_sql_server_connection()
    sql_cursor = sql_conn.cursor()

    last_sql_date = get_sql_server_max_date(table_name)
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    columns = SQL_SERVER_TABLES[table_name]
    select_cols = ", ".join(columns)

    if last_sql_date:
        sqlite_cursor.execute(
            f"SELECT {select_cols} FROM {table_name} WHERE date_time > ? ORDER BY date_time ASC", (last_sql_date,)
        )
    else:
        sqlite_cursor.execute(f"SELECT {select_cols} FROM {table_name} ORDER BY date_time ASC")

    rows = sqlite_cursor.fetchall()
    inserted = 0
    print(f"[db_insert] Sync {table_name}: last_sql_date={last_sql_date}, records_to_sync={len(rows)}")

    if rows:
        # Create temp table
        temp_table = f"#temp_{table_name}"
        create_temp_sql = f"""
        CREATE TABLE {temp_table} (
            {', '.join([f'{col} {get_sql_type(col)}' for col in columns])}
        )
        """
        sql_cursor.execute(create_temp_sql)

        # Bulk insert into temp table in batches
        placeholders = ", ".join(["?" for _ in columns])
        insert_temp_sql = f"INSERT INTO {temp_table} ({', '.join(columns)}) VALUES ({placeholders})"

        batch_count = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            batch_count += 1
            print(f"[db_insert] Sync {table_name}: enviando batch {batch_count} com {len(batch)} registros")
            sql_cursor.executemany(insert_temp_sql, batch)

        # Merge from temp to main table
        merge_sql = f"""
        MERGE dbo.{table_name} AS target
        USING {temp_table} AS source
        ON target.site_id = source.site_id AND target.date_time = source.date_time
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(columns)})
            VALUES ({', '.join([f'source.{col}' for col in columns])});
        """
        sql_cursor.execute(merge_sql)

        # Get count of inserted rows
        sql_cursor.execute(f"SELECT @@ROWCOUNT")
        inserted = sql_cursor.fetchone()[0]

        print(f"[db_insert] Sync {table_name}: inserted={inserted}")

        # Drop temp table
        sql_cursor.execute(f"DROP TABLE {temp_table}")

    sqlite_cursor.close()
    sqlite_conn.close()
    sql_cursor.close()
    sql_conn.close()
    return inserted


def sync_all_tables_to_sql_server(sqlite_db_path="weather.db"):
    total = 0
    print("[db_insert] Iniciando sincronização de todas as tabelas para SQL Server...")
    for table_name in SQL_SERVER_TABLES.keys():
        print(f"[db_insert] Iniciando sync da tabela: {table_name}")
        total += sync_table_to_sql_server(table_name, sqlite_db_path=sqlite_db_path)
    print(f"[db_insert] Sincronização completa, total de registros sincronizados: {total}")
    return total

def create_tables(db_path="weather.db"):
    """Cria tabelas estruturadas para dados de 2025"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Tides
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER, site_name TEXT, date_time DATETIME,
            observed_m REAL, predicted_m REAL, surge_m REAL, msl_m REAL,
            residual_m REAL, stddev REAL, status TEXT,
            quality_percent INTEGER, quality_flag INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')

    # CTD
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ctd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER, site_name TEXT, date_time DATETIME,
            conductivity REAL, temperature_celsius REAL, depth_m REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')

    # Water Quality
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS water_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER, site_name TEXT, date_time DATETIME,
            temperature_celsius REAL, quality_percent INTEGER, records_count INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')

    # Meteorological
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS meteorological (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER, site_name TEXT, date_time DATETIME,
            elevation_m REAL, wind_speed_ms REAL, wind_direction_deg REAL,
            wind_u_ms REAL, wind_v_ms REAL, gust_speed_ms REAL, gust_direction_deg REAL,
            gust_u_ms REAL, gust_v_ms REAL, atmos_pressure_mbar REAL,
            temperature_celsius REAL, humidity_percent REAL, dew_point_celsius REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')

    # Waves
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS waves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER, site_name TEXT, date_time DATETIME,
            wave_height_sig_m REAL, wave_height_max_m REAL, wave_period_peak_s REAL,
            zero_upcross_period_mean_s REAL, wave_period_sig_s REAL, wave_period_mean_s REAL,
            wave_direction_mean_deg REAL, wave_direction_peak_deg REAL,
            directional_spread_mean_deg REAL, total_energy REAL, quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')

    # Air Quality
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS air_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER, site_name TEXT, date_time DATETIME,
            pollutant_level REAL, quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')

    # Currents
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS currents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER, site_name TEXT, date_time DATETIME,
            current_speed_ms REAL, current_direction_deg REAL, depth_m REAL,
            quality_percent INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, date_time)
        )''')

    # Índices
    indices = [
        ("tides", "date_time"), ("ctd", "date_time"), ("water_quality", "date_time"),
        ("air_quality", "date_time"), ("meteorological", "date_time"),
        ("waves", "date_time"), ("currents", "date_time")
    ]

    for table, column in indices:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column})")
        except: pass

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
        val = str(val).replace('%', '').strip()
        return int(float(val))
    except:
        return None

def insert_tides(csv_content, db_path="weather.db"):
    """Insere dados de marés"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader); next(reader)  # Pula headers
    inserted = 0
    for row in reader:
        if not row or len(row) < 12: continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO tides
                (site_id, site_name, date_time, observed_m, predicted_m, surge_m,
                 msl_m, residual_m, stddev, status, quality_percent, quality_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]), row[1], row[2],
                safe_float(row[3]), safe_float(row[4]), safe_float(row[5]),
                safe_float(row[6]), safe_float(row[7]), safe_float(row[8]),
                row[9], safe_int(row[10]), safe_int(row[11])
            ))
            inserted += 1
        except: pass
    conn.commit(); conn.close()
    return inserted

def insert_water_quality(csv_content, db_path="weather.db"):
    """Insere dados de qualidade de água"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader); next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 6: continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO water_quality
                (site_id, site_name, date_time, temperature_celsius, quality_percent, records_count)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]), row[1], row[2],
                safe_float(row[3]), safe_int(row[4]), safe_int(row[5])
            ))
            inserted += 1
        except: pass
    conn.commit(); conn.close()
    return inserted

def insert_meteorological(csv_content, db_path="weather.db"):
    """Insere dados meteorológicos"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader); next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 17: continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO meteorological
                (site_id, site_name, date_time, elevation_m, wind_speed_ms, wind_direction_deg,
                 wind_u_ms, wind_v_ms, gust_speed_ms, gust_direction_deg, gust_u_ms, gust_v_ms,
                 atmos_pressure_mbar, temperature_celsius, humidity_percent, dew_point_celsius, quality_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]), row[1], row[2],
                safe_float(row[3]), safe_float(row[4]), safe_float(row[5]),
                safe_float(row[6]), safe_float(row[7]), safe_float(row[8]),
                safe_float(row[9]), safe_float(row[10]), safe_float(row[11]),
                safe_float(row[12]), safe_float(row[13]), safe_float(row[14]),
                safe_float(row[15]), safe_int(row[16])
            ))
            inserted += 1
        except: pass
    conn.commit(); conn.close()
    return inserted

def insert_waves(csv_content, db_path="weather.db"):
    """Insere dados de ondas"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader); next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 14: continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO waves
                (site_id, site_name, date_time, wave_height_sig_m, wave_height_max_m,
                 wave_period_peak_s, zero_upcross_period_mean_s, wave_period_sig_s, wave_period_mean_s,
                 wave_direction_mean_deg, wave_direction_peak_deg, directional_spread_mean_deg,
                 total_energy, quality_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]), row[1], row[2],
                safe_float(row[3]), safe_float(row[4]), safe_float(row[5]),
                safe_float(row[6]), safe_float(row[7]), safe_float(row[8]),
                safe_float(row[9]), safe_float(row[10]), safe_float(row[11]),
                safe_float(row[12]), safe_int(row[13])
            ))
            inserted += 1
        except: pass
    conn.commit(); conn.close()
    return inserted

def insert_currents(csv_content, db_path="weather.db"):
    """Insere dados de correntes"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 3: continue
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO currents
                (site_id, site_name, date_time, current_speed_ms, current_direction_deg, depth_m, quality_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row[0]), row[1], row[2],
                safe_float(row[3]), safe_float(row[4]),
                safe_float(row[5]), safe_int(row[6]) if len(row) > 6 else None
            ))
            inserted += 1
        except: pass
    conn.commit(); conn.close()
    return inserted

def insert_ctd(csv_content, db_path="weather.db"):
    """Insere dados de CTD"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader); next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 3: continue
        try:
            cursor.execute("INSERT OR IGNORE INTO ctd (site_id, site_name, date_time, quality_percent) VALUES (?, ?, ?, ?)",
                          (int(row[0]), row[1], row[2], safe_int(row[3]) if len(row) > 3 else None))
            inserted += 1
        except: pass
    conn.commit(); conn.close()
    return inserted

def insert_air_quality(csv_content, db_path="weather.db"):
    """Insere dados de qualidade do ar"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    reader = csv.reader(io.StringIO(csv_content))
    next(reader); next(reader)
    inserted = 0
    for row in reader:
        if not row or len(row) < 3: continue
        try:
            cursor.execute("INSERT OR IGNORE INTO air_quality (site_id, site_name, date_time, quality_percent) VALUES (?, ?, ?, ?)",
                          (int(row[0]), row[1], row[2], safe_int(row[3]) if len(row) > 3 else None))
            inserted += 1
        except: pass
    conn.commit(); conn.close()
    return inserted
