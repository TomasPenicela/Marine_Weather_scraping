import sqlite3

conn = sqlite3.connect('weather.db')
cursor = conn.cursor()

print("\n📅 Data Coverage by Dataset\n")

tables_with_data = ['tides', 'water_quality', 'meteorological', 'waves']

for table in tables_with_data:
    cursor.execute(f"SELECT MIN(date_time), MAX(date_time), COUNT(*) FROM {table}")
    min_date, max_date, count = cursor.fetchone()
    print(f"{table:20}: {count:>8,} records")
    print(f"  {'From':20}: {min_date}")
    print(f"  {'To':20}: {max_date}\n")

conn.close()
