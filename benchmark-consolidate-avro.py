import duckdb
import math
import time
from pathlib import Path
import concurrent.futures
import math # Re-import math just in case it wasn't imported, for ceil

DATA_DIR = Path("/home/lazykern/Downloads/ztf-archive/ztf_public_20240201")
all_files = list(DATA_DIR.glob("*.avro"))

MAX_WORKERS = 6
THREADS_PER_WORKER = 2
MEMORY_PER_WORKER = "2GB"
SAFE_MAX_BATCH_SIZE = 22000

total_files = len(all_files)
if total_files == 0:
    print("No files found to process.")
    exit()

ideal_batch_size_float = total_files / MAX_WORKERS
ideal_batch_size = math.ceil(ideal_batch_size_float)
FILES_PER_BATCH = max(1, min(SAFE_MAX_BATCH_SIZE, ideal_batch_size))

num_batches = math.ceil(total_files / FILES_PER_BATCH)

print(f"Total files: {total_files}")
print(f"Calculated files per batch: {FILES_PER_BATCH} (capped at {SAFE_MAX_BATCH_SIZE})")
print(f"Number of batches: {num_batches}")
print(f"Max workers: {MAX_WORKERS}")
print(f"Threads per worker: {THREADS_PER_WORKER}")
print(f"Memory per worker: {MEMORY_PER_WORKER}")

# Prepare the SQL template once
query_template = """
COPY (
    SELECT * EXCLUDE (prv_candidates) FROM read_avro([{}], filename=true)
) TO 's3://ateda-bronze/ztf-alert-archive/2024-02-01/{}.parquet' 
(FORMAT PARQUET, COMPRESSION zstd);
"""


def process_batch(batch_index):
    # Create a new connection for each thread
    batch_con = duckdb.connect()

    batch_con.execute("INSTALL httpfs;")
    batch_con.execute("LOAD httpfs;")
    batch_con.execute("INSTALL json;")
    batch_con.execute("LOAD json;")

    batch_con.execute("SET s3_access_key_id='minioadmin';")
    batch_con.execute("SET s3_secret_access_key='minioadmin';")
    batch_con.execute("SET s3_endpoint='192.168.0.197:9000';")
    batch_con.execute("SET s3_url_style='path';")
    batch_con.execute("SET s3_use_ssl=false;")
    batch_con.execute(f"SET memory_limit='{MEMORY_PER_WORKER}';")
    batch_con.execute(f"SET threads TO {THREADS_PER_WORKER};")
    batch_con.execute("SET temp_directory = '/tmp/duckdb_spill';")
    
    start_index = batch_index * FILES_PER_BATCH
    end_index = min(start_index + FILES_PER_BATCH, total_files)
    batch_files = all_files[start_index:end_index]
    batch_files_str = ",".join(f"'{file}'" for file in batch_files)
    
    q = query_template.format(batch_files_str, batch_index)
    batch_start_time = time.time()
    batch_con.execute(q)
    batch_end_time = time.time()
    print(f"Batch {batch_index} completed in {batch_end_time - batch_start_time} seconds")
    batch_con.close()
    return batch_index

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_batch, i) for i in range(num_batches)]
    for future in concurrent.futures.as_completed(futures):
        print(f"Completed batch {future.result()}")