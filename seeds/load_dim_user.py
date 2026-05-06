import pymysql
import csv

conn = pymysql.connect(host='127.0.0.1', port=9030, user='root')
cursor = conn.cursor()
cursor.execute("USE music_db")

rows = []
with open('d:/code/DEproj02/seeds/dim_user.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        user_id = row[0].replace("'", "''")
        user_name = row[1].replace("'", "''")
        subscription_tier = row[2].replace("'", "''")
        avg_daily_activities = int(row[3])
        registration_date = row[4].replace("'", "''")

        rows.append(f"('{user_id}', '{user_name}', '{subscription_tier}', {avg_daily_activities}, '{registration_date}')")

print(f"Read {len(rows)} rows")

chunk_size = 500
for i in range(0, len(rows), chunk_size):
    chunk = rows[i:i+chunk_size]
    try:
        sql = f"INSERT INTO dim_user VALUES {','.join(chunk)}"
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        for row_val in chunk:
            try:
                sql = f"INSERT INTO dim_user VALUES {row_val}"
                cursor.execute(sql)
                conn.commit()
            except:
                pass
    print(f"Progress: {min(i+chunk_size, len(rows))}/{len(rows)}")

cursor.execute("SELECT COUNT(*) FROM dim_user")
print(f"Final: {cursor.fetchone()[0]} rows")

cursor.close()
conn.close()