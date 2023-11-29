import click
import time
import uuid
from kafka import KafkaProducer, KafkaConsumer


@click.command()
@click.option('--client_type', help='Test: producer or consumer', required=True)
@click.option('--brokers', help='List of brokers.', required=True)
@click.option('--topic', help='Topic to send message to.', required=True)
@click.option('--num_messages', type=click.INT, help='Number of messages to send to broker.', required=True)
@click.option('--msg_size', type=click.INT, help='Size of each message.', required=True)
@click.option('--num_runs', type=click.INT, help='Number of times to run the test.', required=True)
def benchmark(client_type, brokers, topic, num_messages, msg_size, num_runs):
    payload = b"m" * msg_size
    
    if client_type == 'producer':
        client = KafkaProducer(bootstrap_servers=brokers)
        benchmark_fn = _produce
    elif client_type == 'consumer':
        client = KafkaConsumer(topic, bootstrap_servers=brokers, group_id=str(uuid.uuid1()), auto_offset_reset="earliest")
        client.subscribe([topic])
        benchmark_fn = _consume
    
    print(f"Starting benchmark for Kafka-Python {client_type}.")
    
    run_times = []
    for _ in range(num_runs):
        run_start_time = time.time()
        benchmark_fn(client, topic, payload, num_messages)
        run_time_taken = time.time() - run_start_time
        run_times.append(run_time_taken)

    print_results(
        f"Kafka-Python {client_type}", run_times, num_messages, msg_size)


def _produce(producer, topic, payload, num_messages):
    for _ in range(num_messages):
        try:
            producer.send(topic, payload)
        except Exception:
            pass
    
    # Wait until all messages have been delivered
    # sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    producer.flush()

def _consume(consumer, topic, payload, num_messages):
    num_messages_consumed = 0
    for msg in consumer:
        num_messages_consumed += 1        
        if num_messages_consumed >= num_messages:
            break

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
