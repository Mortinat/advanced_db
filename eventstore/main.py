# docker run -d --name eventstoredb-insecure -it -p 2113:2113 eventstore/eventstore:21.10.9-buster-slim --insecure
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
        for i in range(40):
            list_events.append(NewEvent(type='OrderCreated', data=b'{"order_number": "123456"}'))

        commit_position1 = client.append_to_stream(
            stream_name=stream_name1,
            current_version=StreamState.ANY,
            events=list_events,
        )
    list_time.append(time.time() - start)
    print(f'time_{k}: {list_time[-1]}')

print(f'avg: {sum(list_time)/len(list_time)}')


# Update benchmark
list_update_time = []
for k in range(6):
    start = time.time()
    for j in range(40):
        read_response = client.read_stream(stream_name=stream_name1)
        events = read_response.stream.events
        for event in events:
            # Get the event position to perform an update
            event_position = event.event_number
            updated_data = event.data + b" - Updated"
            updated_event = NewEvent(type=event.type, data=updated_data)
            client.append_to_stream(
                stream_name=stream_name1,
                expected_version=event_position,
                events=[updated_event],
            )
    list_update_time.append(time.time() - start)
    print(f'update_time_{k}: {list_update_time[-1]}')

print(f'update_avg: {sum(list_update_time)/len(list_update_time)}')





client.close()