# docker run -d --name eventstoredb-insecure -it -p 2113:2113 eventstore/eventstore:21.10.9-buster-slim --insecure
import random
import time
import threading

from esdbclient import EventStoreDBClient, NewEvent, StreamState
# issue 10000000
NUM_ROWS = [10000, 100000, 1000000, 10000000]

def insert_data(size):
    numb_of_loops = size // 10000
    for j in range(numb_of_loops+1):
        list_events = [NewEvent(type='OrderCreated', data=b'a'*100)] * 10000

        commit_position1 = client.append_to_stream(
            stream_name=stream_name1,
            current_version=StreamState.ANY,
            events=list_events,
        )

if __name__ == "__main__":
    dict_time = {}
    client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )

    stream_name1 = 'test-stream-1'

    for num_row in NUM_ROWS:
        list_time = []
        for k in range(6):
            thread_list = []
            temp = num_row//5
            for i in range(5):
                thread_list.append(threading.Thread(target=insert_data, args=(temp, )))
            # insert_data(10000000)
            start = time.time()
            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()
            list_time.append(time.time() - start)
            print(f'time_{k}: {list_time[-1]}')
            client.delete_stream(stream_name=stream_name1, current_version=StreamState.ANY)

        avg = sum(list_time)/len(list_time)
        dict_time[num_row] = avg
        print(f'avg: {sum(list_time)/len(list_time)}')
    print(dict_time)


# DOES NOT WORK
# Update benchmark
# list_update_time = []
# for k in range(6):
#     start = time.time()
#     for j in range(40):
#         read_response = client.read_stream(stream_name=stream_name1)
#         events = read_response
#         for event in events:
#             # Get the event position to perform an update
#             event_position = event.stream_position
#             updated_data = event.data + b" - Updated"
#             updated_event = NewEvent(type=event.type, data=updated_data)
#             client.append_to_stream(
#                 stream_name=stream_name1,
#                 current_version=event_position,
#                 events=[updated_event],
#             )
#     list_update_time.append(time.time() - start)
#     print(f'update_time_{k}: {list_update_time[-1]}')

# print(f'update_avg: {sum(list_update_time)/len(list_update_time)}')





client.close()