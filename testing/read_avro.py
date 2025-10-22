from fastavro import reader

with open("./streaming.public.customers.avro", "rb") as f:
    avro_reader = reader(f)
    for record in avro_reader:
        print(record)
