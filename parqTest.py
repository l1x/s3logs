import pyarrow.parquet as pq
import sys

parquet_file = pq.ParquetFile(sys.argv[0])

print(parquet_file.metadata)

print(parquet_file.schema)


