import duckdb
import timeit
import statistics

conn = duckdb.connect(database=':memory:', read_only=False)

conn.execute('''
    CREATE TABLE trips (
        ID INTEGER,
        VendorID INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count FLOAT,
        trip_distance FLOAT,
        RatecodeID FLOAT,
        store_and_fwd_flag VARCHAR,
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
        another_airport_fee FLOAT
    )
''')
file_name = 'C:\\Users\\Slowpoker\\PycharmProjects\\bd_lab_3\\nyc_yellow_big.csv'
conn.execute(
    f"COPY trips FROM '{file_name}' (DELIMITER ',', NULL '', HEADER TRUE)")

queries = [
    "SELECT VendorID, count(*) FROM trips GROUP BY 1",
    "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1",
    "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2",
    "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"
]
print('duckdb big_file')
for i, query in enumerate(queries, start=1):
    times = []
    for _ in range(10):
        start_time = timeit.default_timer()
        result = conn.execute(query).fetchall()
        end_time = timeit.default_timer()
        times.append(end_time - start_time)
    print(f"query {i}: {statistics.median(times)} seconds")

conn.close()
