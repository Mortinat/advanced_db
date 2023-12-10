import psycopg2
import psycopg2.extras
import time
import threading

msg = b'a'*100
NUM_ROWS = [1000, 10000, 100000, 1000000]
NUM_THREADS = [1, 5, 10]


def insert_data(cur, conn, inser_query, list_data):
    psycopg2.extras.execute_values(cur, insert_query, list_data, template=None, page_size=25000)
    conn.commit()

def read_data(cur, conn, read_query, num_row):
    cur.execute(read_query, (num_row, ))
    conn.commit()

if __name__ == "__main__":
    dict_w_time = {}
    dict_r_time = {}
    conn = psycopg2.connect("dbname=advance_db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS table1 (id serial PRIMARY KEY, num integer, data varchar);")

    for num_row in NUM_ROWS:
        list_data = [(i, msg) for i in range(num_row)]
        insert_query = "INSERT INTO table1 (num, data) VALUES %s;"
        read_query = "SELECT * FROM table1 limit %s;"
        for num_thread in NUM_THREADS:
            list_w_time = []
            list_r_time = []
            for k in range(6):
                thread_list = []
                temp = num_row//num_thread
                for i in range(num_thread):
                    thread_list.append(threading.Thread(target=insert_data, args=(cur, conn, insert_query, list_data[i*temp:(i+1)*temp])))
                start = time.time()
                for thread in thread_list:
                    thread.start()
                for thread in thread_list:
                    thread.join()
                list_w_time.append(time.time() - start)

                thread_list = []
                for i in range(num_thread):
                    thread_list.append(threading.Thread(target=read_data, args=(cur, conn, read_query, temp)))
                start = time.time()
                for thread in thread_list:
                    thread.start()
                for thread in thread_list:
                    thread.join()
                list_r_time.append(time.time() - start)
                
                cur.execute("DELETE FROM table1;")
                conn.commit()
            avg = sum(list_w_time)/len(list_w_time)
            dict_w_time[f"{num_row}_{num_thread}"] = avg
            print(f'avg_w: {avg}')
            
            avg = sum(list_r_time)/len(list_r_time)
            dict_r_time[f"{num_row}_{num_thread}"] = avg
            print(f'avg_r: {avg}')

    print(dict_w_time)
    print(dict_r_time)
    cur.close()
    conn.close()
