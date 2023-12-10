import click
import time
import uuid
import threading
import matplotlib.pyplot as plt
from kafka import KafkaProducer, KafkaConsumer

@click.command()
@click.option('--client_type', help='Test: producer or consumer or both', required=True)
@click.option('--brokers', help='List of brokers.', required=True)
@click.option('--topic', help='Topic to send message to.', required=True)
@click.option('--num_messages', type=click.INT, help='Number of messages to send to broker.', required=False)
@click.option('--msg_size', type=click.INT, help='Size of each message.', required=True)
@click.option('--num_runs', type=click.INT, help='Number of times to run the test.', required=True)
@click.option('--num_producers', default=1, type=click.INT, help='Number of producer threads to create.', required=False)

def benchmark(client_type, brokers, topic, num_messages, msg_size, num_runs, num_producers):
    payload = b"x" * msg_size
    avg_times = []
    message_counts = [10000,100000,1000000,10000000]
    for _ in message_counts:
        print(f"Testing with {_} messages")
        run_times = []

        for __ in range(num_runs):
            start_time = time.time()
            # Assuming _produce_multi_threaded is defined as in your earlier setup
            if client_type == 'both':
                producer_thread = threading.Thread(target=_produce_multi_threaded,
                                                   args=(brokers, topic, payload, _, num_producers))
                consumer_thread = threading.Thread(target=_consume, args=(brokers, topic, _))
                producer_thread.start()
                consumer_thread.start()
                producer_thread.join()
                consumer_thread.join()
            elif client_type == 'producer':
                _produce_multi_threaded(brokers, topic, payload, _, num_producers)
            elif client_type == 'consumer':
                _consume_multi_threaded(brokers, topic, _, num_producers)
            run_time_taken = time.time() - start_time
            run_times.append(run_time_taken)
        avg_run_time = sum(run_times) / num_runs
        avg_times.append(avg_run_time)
        print_results(f"Kafka-Python {client_type}", run_times, _, msg_size)

    plot_message_count_vs_time(message_counts,avg_times, num_producers)

def _produce_multi_threaded(brokers, topic, payload, num_messages, num_producers):
    threads = []
    for _ in range(num_producers):
        producer = KafkaProducer(bootstrap_servers=brokers)
        thread = threading.Thread(target=_produce, args=(producer, topic, payload, num_messages))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def _produce(producer, topic, payload, num_messages):

    for _ in range(num_messages):
        try:
            producer.send(topic, payload)
        except Exception:
            pass
    producer.flush()

def _consume(brokers, topic, num_messages):
    num_messages_consumed = 0
    for msg in brokers:
        num_messages_consumed += 1
        if num_messages_consumed >= num_messages:
            break

def _consume_multi_threaded(brokers, topic, num_messages, num_consumers):
    threads = []
    messages_per_consumer = num_messages // num_consumers
    for _ in range(num_consumers):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=str(uuid.uuid1()),
            auto_offset_reset="earliest"
        )
        thread = threading.Thread(target=_consume, args=(consumer, messages_per_consumer))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()



def plot_message_count_vs_time(message_sizes, avg_times,users):
    filename= "message_count_nbusers"\
             +str(users)+"vs_time"+".png"

    plt.figure(figsize=(10, 6))
    plt.plot(message_sizes, avg_times, marker='o')
    plt.title('Average Time for Different Message Counts ' + "; Nombre de user = " + str(users))
    plt.xlabel('Number of Messages')
    plt.ylabel('Average Time (seconds)')
    plt.grid(True)
    plt.savefig(filename)
    plt.close()


def print_results(test_name, run_times, num_messages, msg_size):
    print(f"{test_name} Results:")
    print(f"Number of Runs: {len(run_times)}, "
          f"Number of messages: {num_messages}, "
          f"Message Size: {msg_size} bytes.")

    total_run_times = sum(run_times)
    time_to_send_messages = total_run_times / len(run_times)
    messages_per_sec = len(run_times) * num_messages / total_run_times
    mb_per_sec = messages_per_sec * msg_size / (1024 * 1024)

    print(f"Average Time for {num_messages} messages: {time_to_send_messages} seconds.")
    print(f"Messages / sec: {messages_per_sec}")
    print(f"MB / sec : {mb_per_sec}")



if __name__ == '__main__':
    benchmark()

