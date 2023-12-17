import sqlite3
import csv
import timeit
import statistics

# Создаем подключение к базе данных SQLite
conn = sqlite3.connect(':memory:')
c = conn.cursor()

# Создаем таблицу
c.execute('''
    CREATE TABLE trips (
        ID INTEGER PRIMARY KEY,
        VendorID INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count FLOAT,
        trip_distance FLOAT,
        RatecodeID FLOAT,
        store_and_fwd_flag TEXT,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        payment_type INTEGER,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT,
        airport_fee FLOAT,
        another_airport_fee NULL
      
    )
''')
file_name = 'C:\\Users\\Slowpoker\\PycharmProjects\\bd_lab_3\\nyc_yellow_big.csv'
f = open(file_name)
# Читаем CSV-файл и записываем его содержимое в таблицу
# with open('C:\\Users\\Slowpoker\\PycharmProjects\\bd_lab_3\\nyc_yellow_big.csv', 'r') as fin:
#     dr = csv.reader(fin)
#     to_db = [row for row in dr]
f.readline()
for row in f:
    c.execute(
        "INSERT INTO trips  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
        row.split(','))
conn.commit()

# Запросы
queries = [
    "SELECT VendorID, count(*) FROM trips GROUP BY 1",
    "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1",
    "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2",
    "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"
]
print('sqlite big_file')
for i, query in enumerate(queries, start=1):
    times = []
    for _ in range(10):
        start_time = timeit.default_timer()
        c.execute(query)
        result = c.fetchall()
        end_time = timeit.default_timer()
        times.append(end_time - start_time)
    print(f"query {i}: {statistics.median(times)} seconds")

# Закрываем подключение к базе данных
conn.close()
