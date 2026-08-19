"""
Publishes tweet text into a Kafka topic, one message every `--delay`
seconds, so the Spark Structured Streaming notebook has real data to
classify (instead of the fake `rate` source it used before).

Setup (once):
    pip install kafka-python

Usage:
    python producer.py                       # uses dataset.csv if present
    python producer.py --file dataset.csv --topic tweets-input --delay 1
"""
import argparse
import csv
import time

from kafka import KafkaProducer

# Used only if dataset.csv can't be found, so the script still works out
# of the box for a quick smoke test.
SAMPLE_TWEETS = [
    "I absolutely love this new phone, best purchase ever!",
    "This service is terrible, I want a refund immediately.",
    "Not sure if this update actually fixed anything, still testing it out.",
    "The company is being sued for allegedly breaching the contract.",
    "Had a wonderful time at the concert tonight, amazing energy!",
    "Why does the app keep crashing every single time I open it.",
    "We are still evaluating the vendor before signing anything.",
    "Court documents reveal new evidence in the ongoing litigation.",
]


def load_tweets(path):
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            tweets = [row["Text"] for row in reader if row.get("Text")]
        if tweets:
            print(f"Loaded {len(tweets)} tweets from '{path}'.")
            return tweets
    except FileNotFoundError:
        pass
    print(f"Could not read tweets from '{path}', using "
          f"{len(SAMPLE_TWEETS)} built-in sample tweets instead.")
    return SAMPLE_TWEETS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="tweets-input")
    parser.add_argument("--file", default="dataset.csv")
    parser.add_argument("--delay", type=float, default=1.0,
                         help="seconds to wait between messages")
    parser.add_argument("--limit", type=int, default=0,
                         help="only send the first N tweets (0 = all)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    tweets = load_tweets(args.file)
    if args.limit:
        tweets = tweets[: args.limit]

    print(f"Publishing {len(tweets)} tweets to topic '{args.topic}' "
          f"every {args.delay}s (Ctrl+C to stop early)...")

    try:
        for i, tweet in enumerate(tweets):
            producer.send(args.topic, tweet)
            producer.flush()
            print(f"[{i}] sent: {tweet[:80]!r}")
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nStopped by user.")

    print("Done.")


if __name__ == "__main__":
    main()
