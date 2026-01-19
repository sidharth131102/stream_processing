import apache_beam as beam

def write_pubsub(pcoll, topic):
    pcoll | "WritePubSub" >> beam.io.WriteToPubSub(topic)
