import random
import time

from esdbclient import EventStoreDBClient, NewEvent, StreamState


client = EventStoreDBClient(
    uri="esdb://localhost:2113?Tls=false"
)

stream_name1 = 'test-stream-1'

list_time = []
for k in range(6):
    start = time.time()
    for j in range(40):
        list_events = []
        for i in range(25000):
            list_events.append(NewEvent(type='OrderCreated', data=b'{"order_number": "123456"}'))

        commit_position1 = client.append_to_stream(
            stream_name=stream_name1,
            current_version=StreamState.ANY,
            events=list_events,
        )
    list_time.append(time.time() - start)
    print(f'time_{k}: {list_time[-1]}')

print(f'avg: {sum(list_time)/len(list_time)}')


client.close()