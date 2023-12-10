import psycopg2
import psycopg2.extras
import time
import threading

msg = b'a'*100
NUM_ROWS = [10000, 100000, 1000000, 10000000]

def insert_data(cur, conn, inser_query, list_data):
    semq.acquire()
    psycopg2.extras.execute_values(cur, insert_query, list_data, template=None, page_size=25000)
    con.commit()
    semq.release()
    

if __name__ == "__main__":
    dict_time = {}
    conn = psycopg2.connect("dbname=advance_db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS table1 (id serial PRIMARY KEY, num integer, data varchar);")

    for num_row in NUM_ROWS:
        list_time = []
        list_data = [(i, msg) for i in range(num_row)]
        insert_query = "INSERT INTO table1 (num, data) VALUES %s"
        sema = threading.Semaphore(1)
        for k in range(6):
            cur.execute("SELECT * FROM table1;")
            thread_list = []
            temp = num_row//5
            for i in range(5):
                thread_list.append(threading.Thread(target=insert_data, args=(cur, conn, insert_query, list_data[i*temp:(i+1)*temp])))
            start = time.time()
            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()
            list_time.append(time.time() - start)
            print(f'time_{k}: {list_time[-1]}')
            cur.execute("DELETE FROM table1;")
            conn.commit()

        avg = sum(list_time)/len(list_time)
        dict_time[num_row] = avg
        print(f'avg: {sum(list_time)/len(list_time)}')
    print(dict_time)