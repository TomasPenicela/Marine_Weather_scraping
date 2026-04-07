import sqlite3

conn = sqlite3.connect('weather.db')
cursor = conn.cursor()

# Get list of tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [row[0] for row in cursor.fetchall()]

print(f"\n📊 Database Status\n")
print(f"Tables found: {len(tables)}")
print(f"Tables: {', '.join(tables)}\n")

print("Record counts:")
total_records = 0
for table in sorted(tables):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        total_records += count
        print(f"  {table:20} : {count:>10,} records")
    except Exception as e:
        print(f"  {table:20} : ERROR - {e}")

print(f"\n{'TOTAL':20} : {total_records:>10,} records\n")

conn.close()
