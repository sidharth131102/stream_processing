import apache_beam as beam

def read_pubsub(p, subscription):
    return (
        p
        | "ReadPubSub" >> beam.io.ReadFromPubSub(
            subscription=subscription,
            with_attributes=True
        )
    )
