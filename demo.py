import time
from subscriber import Subscriber
from publisher import Publisher

CHANNELS = ["news", "sports"]

def main():
    # Start subscriber in background thread
    sub = Subscriber(channels=CHANNELS)
    sub.start()

    # Give subscriber time to connect and subscribe
    time.sleep(0.5)

    pub = Publisher()

    messages = [
        ("news",   "Breaking: Redis Pub/Sub demo is live!"),
        ("sports", "Goal! Python scores with threading!"),
        ("news",   "Update: Everything is working perfectly."),
        ("sports", "Final whistle — demo complete."),
    ]

    for channel, msg in messages:
        pub.publish(channel, msg)
        time.sleep(0.5)

    # Let the last messages be received before stopping
    time.sleep(0.5)
    sub.stop()
    print("[Demo] Done.")


if __name__ == "__main__":
    main()