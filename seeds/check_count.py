import pymysql
conn = pymysql.connect(host='127.0.0.1', port=9030, user='root')
cursor = conn.cursor()
cursor.execute("USE music_db")

# Check current count
cursor.execute("SELECT COUNT(*) FROM dim_track")
print(f"Current rows: {cursor.fetchone()[0]}")

conn.close()