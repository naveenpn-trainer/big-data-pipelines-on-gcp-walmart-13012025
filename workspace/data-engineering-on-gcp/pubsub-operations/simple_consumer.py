from google.cloud import pubsub_v1


def callback_fn(message):
    print(f"Message= {message.data.decode('utf-8')}")
    message.ack()


if __name__ == '__main__':
    publisher = pubsub_v1.SubscriberClient()
    topic_name = "projects/red-splice-429501-q7/topics/my-first-topic"
    subscription_id = "projects/red-splice-429501-q7/subscriptions/my-first-topic-sub"

    future = publisher.subscribe(subscription_id, callback=callback_fn)
    print(future.result())
