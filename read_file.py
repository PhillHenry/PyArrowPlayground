import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs, RecordBatchStreamReader

# Replace 'your_arrow_stream' with the actual Arrow stream
# This could be a BytesIO object containing Arrow data
filename = '/dev/shm/ping'

# Create an Arrow InputStream from the stream
def read_from_shm():
    with pa.ipc.RecordBatchFileReader(filename) as source:
        print(source.num_record_batches)
        print(source.schema)
        print(source.get_record_batch(0))
        bytes = source.read_all()
        df = pa.ipc.deserialize_pandas(bytes)
        print(df)


def read_from_shm2():
    local = fs.LocalFileSystem()
    with local.open_input_stream(filename) as source:
        bytes = source.readall()
        print(len(bytes))
        reader = RecordBatchStreamReader(bytes)
        reader.read_pandas()
        df = pa.ipc.deserialize_pandas(bytes)
        print(df)

    with pa.ipc.RecordBatchFileReader(filename) as reader:
        record_batch = reader.read_all()
        print(record_batch)


def read_from_shm3():
    local = fs.LocalFileSystem()
    with local.open_input_file(filename) as source:
        pa.ipc.read_message(source)
        schema = pa.ipc.read_schema(source)
        record_batch = pa.ipc.read_record_batch(source, schema)


def read_table():
    with pq.read_table(filename) as source:  # Parquet magic bytes not found in footer. Either the file is corrupted or this is not a parquet file.
        print(source)


if __name__ == "__main__":
    read_from_shm()
