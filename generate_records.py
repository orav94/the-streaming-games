import time
import uuid
import random
import json
from datetime import datetime, timezone
import heapq  # We'll use a min-heap for scheduling

from confluent_kafka import Producer

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 500000
}

BIDS_PER_SECOND = 150000       # Number of bids to generate each second
PROBABILITY_WON = 0.75          # Probability that a bid is "won"
PROBABILITY_RENDER = 0.35       # Probability that a won bid eventually gets a render
MAX_RESPONSE_DELAY = 10        # Up to 10 seconds after bid creation
MAX_RENDER_DELAY = 50          # Up to 50 seconds after a won response

BIDS_TOPIC = "bids"
RESPONSES_TOPIC = "responses"
RENDERS_TOPIC = "renders"

def produce_message(producer, topic, key, value):
    """
    Sends a message to Kafka using the confluent-kafka Producer.
    
    :param producer: confluent_kafka.Producer instance
    :param topic: str, the Kafka topic
    :param key: str, the message key
    :param value: dict, the message value (will be JSON-serialized)
    """
    # Use JSON for structured data:
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(value).encode('utf-8')
    )

###################################
# Main Logic
###################################

def main():
    producer = Producer(KAFKA_CONFIG)

    # Priority queues (min-heaps) for scheduling future sends:
    # Each entry in the queue is (due_time, data...)
    response_schedule = []
    render_schedule = []

    last_flush_time = time.time()

    while True:
        loop_start = time.time()

        # 1. Generate new BIDS
        for _ in range(BIDS_PER_SECOND):
            sid = str(uuid.uuid4())
            bid_time = datetime.now(timezone.utc).isoformat()

            # Produce a BID record immediately
            produce_message(
                producer,
                BIDS_TOPIC,
                key=sid,
                value={"sid": sid, "bid_time": bid_time}
            )

            # Determine if the bid will be won
            won = (random.random() < PROBABILITY_WON)

            # Schedule a RESPONSE within 0..MAX_RESPONSE_DELAY seconds
            response_delay = random.random() * MAX_RESPONSE_DELAY
            response_due_time = loop_start + response_delay
            heapq.heappush(response_schedule, (response_due_time, sid, won))

        # 2. Send any due RESPONSES
        now = time.time()
        while response_schedule and response_schedule[0][0] <= now:
            _, sid, won = heapq.heappop(response_schedule)

            # Produce RESPONSE record
            produce_message(
                producer,
                RESPONSES_TOPIC,
                key=sid,
                value={"sid": sid, "won": won}
            )

            # If won, schedule a possible RENDER
            if won and (random.random() < PROBABILITY_RENDER):
                render_delay = random.random() * MAX_RENDER_DELAY
                render_due_time = now + render_delay
                heapq.heappush(render_schedule, (render_due_time, sid))

        # 3. Send any due RENDERS
        now = time.time()
        while render_schedule and render_schedule[0][0] <= now:
            _, sid = heapq.heappop(render_schedule)

            # Produce RENDER record
            produce_message(
                producer,
                RENDERS_TOPIC,
                key=sid,
                value={"sid": sid, "rendered": True}
            )

        # 4. Let the producer handle any background sending tasks
        producer.poll(0)

        # 5. Periodically flush to ensure messages are delivered
        if time.time() - last_flush_time >= 5:  # for example, every 5 seconds
            producer.flush()
            last_flush_time = time.time()

        # 6. Sleep the remainder of the second to maintain bids rate
        elapsed = time.time() - loop_start
        sleep_time = 1.0 - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()

