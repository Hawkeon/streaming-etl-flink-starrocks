import pymysql
import csv

conn = pymysql.connect(host='127.0.0.1', port=9030, user='root')
cursor = conn.cursor()
cursor.execute("USE music_db")

rows = []
with open('d:/code/DEproj02/seeds/dim_track_clean.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        track_id = row[0].replace("'", "''")
        track_name = row[1].replace("'", "''")
        artists = row[2].replace("'", "''")
        album_name = row[3].replace("'", "''")
        popularity = int(row[4])
        duration_ms = int(row[5])
        explicit = row[6] == '1'
        danceability = float(row[7])
        energy = float(row[8])
        key = int(row[9])
        loudness = float(row[10])
        mode = int(row[11])
        speechiness = float(row[12])
        acousticness = float(row[13])
        instrumentalness = float(row[14])
        liveness = float(row[15])
        valence = float(row[16])
        tempo = float(row[17])
        time_signature = int(row[18])
        track_genre = row[19].replace("'", "''")

        rows.append(f"('{track_id}', '{track_name}', '{artists}', '{album_name}', {popularity}, {duration_ms}, {explicit}, {danceability}, {energy}, {key}, {loudness}, {mode}, {speechiness}, {acousticness}, {instrumentalness}, {liveness}, {valence}, {tempo}, {time_signature}, '{track_genre}')")

print(f"Read {len(rows)} rows")

chunk_size = 500
skipped = 0
for i in range(0, len(rows), chunk_size):
    chunk = rows[i:i+chunk_size]
    try:
        sql = f"INSERT INTO dim_track VALUES {','.join(chunk)}"
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        for row_val in chunk:
            try:
                sql = f"INSERT INTO dim_track VALUES {row_val}"
                cursor.execute(sql)
                conn.commit()
            except:
                skipped += 1
    print(f"Progress: {min(i+chunk_size, len(rows))}/{len(rows)} (skipped: {skipped})")

cursor.execute("SELECT COUNT(*) FROM dim_track")
print(f"Final: {cursor.fetchone()[0]} rows (skipped: {skipped})")

cursor.close()
conn.close()