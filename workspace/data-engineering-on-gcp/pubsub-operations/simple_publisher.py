from google.cloud import pubsub_v1
if __name__ == '__main__':
  publisher = pubsub_v1.PublisherClient()
  topic_name = "projects/red-splice-429501-q7/topics/my-first-topic"
  data = '{"name":"Naveen Pn"}'
  message = bytes(data,'utf-8')
  future = publisher.publish(topic_name,message)
  print(future.result())

