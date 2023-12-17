import pandas as pd
from sqlalchemy import create_engine, text
import  sqlalchemy as al
import csv
import timeit
import statistics

engine = al.create_engine('sqlite:///:memory:', echo=True)
file_name ='C:\\Users\\Slowpoker\\PycharmProjects\\bd_lab_3\\nyc_yellow_big.csv'
data = pd.read_csv(file_name, delimiter=',')
df = pd.DataFrame(data)
df.columns = ['ID', 'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee', 'another_airport_fee']
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df.to_sql('trips', engine, if_exists='append', index=False)

queries = [
    "SELECT VendorID, count(*) FROM trips GROUP BY 1",
    "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1",
    "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2",
    "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"
]

ans = []
for i, query in enumerate(queries, start=1):
    times = []
    for _ in range(10):
        start_time = timeit.default_timer()
        df = pd.read_sql_query(query, con=engine)
        end_time = timeit.default_timer()
        times.append(end_time - start_time)
    ans.append(statistics.median(times))
print('pandas big_file')
for i in range(len(ans)):
    print(f"query {i+1}: {ans[i]} seconds")