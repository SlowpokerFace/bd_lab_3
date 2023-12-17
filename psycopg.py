import psycopg2
import csv
import timeit
import statistics

conn = psycopg2.connect(database="postgres", user="postgres", password="superman", host="localhost", port="5432")

cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS trips")

cur.execute('''
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
    fare_amount FLOAT8,
    extra FLOAT8,
    mta_tax FLOAT8,
    tip_amount FLOAT8,
    tolls_amount FLOAT8,
    improvement_surcharge FLOAT8,
    total_amount FLOAT8,
    congestion_surcharge FLOAT8,
    airport_fee FLOAT8,
    another_airport_fee FLOAT8
)
''')
file_name ='nyc_yellow_big.csv'
with open(file_name, 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        row = [None if value == '' else value for value in row]
        cur.execute(
            "INSERT INTO trips VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            row
        )

conn.commit()

queries = [
    "SELECT VendorID, count(*) FROM trips GROUP BY 1",
    "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1",
    "SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2",
    "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"
]
print('psycopg tiny_file')
for i, query in enumerate(queries, start=1):
    times = []
    for _ in range(10):
        start_time = timeit.default_timer()
        cur.execute(query)
        end_time = timeit.default_timer()
        times.append(end_time - start_time)
    print(f"query {i}: {statistics.median(times)} seconds")

conn.close()
