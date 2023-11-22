import pyarrow as pa

# Replace 'your_arrow_stream' with the actual Arrow stream
# This could be a BytesIO object containing Arrow data
filename = '/dev/shm/arrow'

# Create an Arrow InputStream from the stream
def read_from_shm():
    with pa.input_stream(filename) as stream:
        input_stream = stream.read()
        reader = pa.ipc.open_stream(input_stream)
        schema = reader.schema
        record_batch = reader.read_next_batch()
        print(record_batch)
        print(record_batch.column(0).tolist())
        df = record_batch.to_pandas()
        print(df)


if __name__ == "__main__":
    read_from_shm()
