import pyarrow as pa

# Replace 'your_arrow_stream' with the actual Arrow stream
# This could be a BytesIO object containing Arrow data
filename = '/dev/shm/arrow'

# Create an Arrow InputStream from the stream
with pa.input_stream(filename) as stream:
    input_stream = stream.read()
    reader = pa.ipc.open_stream(input_stream)
    record_batch = reader.read_all()
    df = record_batch.to_pandas()
    print(df)

    # Read the Arrow stream
    record_batch = pa.ipc.read_record_batch(input_stream)

    # Convert the RecordBatch to a Pandas DataFrame
    df = record_batch.to_pandas()

    # Print the DataFrame
    print(df)
