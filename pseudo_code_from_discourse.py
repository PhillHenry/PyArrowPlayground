import pyarrow as pa
import polars as pl
import S3FileSystem from  s3fs

fs = S3FileSystem()

# Format: {prefix}/data_{fileno}.snappy.parquet
input_dataset = pa.dataset.dataset(
    s3_input.removeprefix("s3://"),
    schema=input_schema,
    format="parquet",
    filesystem=fs,
)

output_lazy = (
    pl.scan_pyarrow_dataset(input_dataset)
    .sort("DEVICE_ID", "PROCESSING_TIME")
    .group_by("DEVICE_ID")
    .map_groups(
        partial(_process_device_records, algo),
        output_schema,
    )
)
# Polars doesn't support writing LazyFrame directly to Parquet, so we need to
# collect everything in memory.
output_data = output_lazy.collect(streaming=True)
logger.info("Done.")

logger.info("Converting output to Arrow Table.")
output_arrow_table = output_data.to_arrow()
logger.info("Done.")

logger.info("Saving output to S3.")
output_file_options = pa.dataset.ParquetFileFormat().make_write_options(compression="snappy")
pa.dataset.write_dataset(
    output_arrow_table,
    s3_output.removeprefix("s3://"),
    format="parquet",
    filesystem=fs,
    file_options=output_file_options,
    max_rows_per_group=max_rows_per_group,
    max_rows_per_file=max_rows_per_output_file,
)
logger.info("Done.")

logger.info("Saving completion marker to S3 output.")
fs_output.touch(s3_completed_marker.removeprefix("s3://"))
logger.info("Done.")