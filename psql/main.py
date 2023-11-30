import psycopg2
import psycopg2.extras
import time

conn = psycopg2.connect("dbname=advance_db")
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS table1 (id serial PRIMARY KEY, num integer, data varchar);")

cur.execute("INSERT INTO table1 (num, data) VALUES (%s, %s)", (100, "abc'def"))
cur.execute("SELECT * FROM table1;")
print(cur.fetchone())

insert_query = "INSERT INTO table1 (num, data) VALUES %s"
list_data = [(i, f'abc{i}') for i in range(100000)]
list_time = []
for i in range(6):
    start = time.time()
    psycopg2.extras.execute_values(cur, insert_query, list_data, template=None, page_size=25000)
    list_time.append(time.time() - start)
    print(f'insert time: {list_time[-1]}')
print(f'avg: {sum(list_time)/len(list_time)}')