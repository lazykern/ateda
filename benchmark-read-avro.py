import duckdb
import math
import time
from pathlib import Path
import concurrent.futures
import argparse # Import argparse
import math # Re-import math just in case it wasn't imported, for ceil

DATA_DIR = Path("/home/lazykern/Downloads/ztf-archive/ztf_public_20240201")
all_files = list(DATA_DIR.glob("*.avro"))

# --- Default Configuration (can be overridden by args) ---
DEFAULT_MAX_WORKERS = 6
DEFAULT_THREADS_PER_WORKER = 2
DEFAULT_MEMORY_PER_WORKER = "2GB"
DEFAULT_SAFE_MAX_BATCH_SIZE = 22000

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Benchmark Avro file reading with DuckDB.")
parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS,
                    help=f"Number of parallel workers (ThreadPoolExecutor max_workers). Default: {DEFAULT_MAX_WORKERS}")
parser.add_argument("--threads", type=int, default=DEFAULT_THREADS_PER_WORKER,
                    help=f"Number of threads per DuckDB worker. Default: {DEFAULT_THREADS_PER_WORKER}")
parser.add_argument("--memory", type=str, default=DEFAULT_MEMORY_PER_WORKER,
                    help=f"Memory limit per DuckDB worker (e.g., '2GB'). Default: {DEFAULT_MEMORY_PER_WORKER}")
parser.add_argument("--batch_size", type=int, default=None,
                    help="Fixed number of files per batch. If not set, calculated dynamically capped at SAFE_MAX_BATCH_SIZE.")
parser.add_argument("--safe_max_batch_size", type=int, default=DEFAULT_SAFE_MAX_BATCH_SIZE,
                    help=f"Cap for dynamic batch size calculation if --batch_size is not set. Default: {DEFAULT_SAFE_MAX_BATCH_SIZE}")

args = parser.parse_args()

# Use parsed arguments
MAX_WORKERS = args.workers
THREADS_PER_WORKER = args.threads
MEMORY_PER_WORKER = args.memory
SAFE_MAX_BATCH_SIZE = args.safe_max_batch_size

# --- Dynamic Batch Size Calculation (if not specified) ---
total_files = len(all_files)
if total_files == 0:
    print("No files found to process.")
    exit()

if args.batch_size is not None:
    FILES_PER_BATCH = max(1, args.batch_size) # Use fixed batch size if provided
    batch_size_source = "fixed via argument"
else:
    # Calculate dynamic batch size capped at SAFE_MAX_BATCH_SIZE
    ideal_batch_size_float = total_files / MAX_WORKERS
    ideal_batch_size = math.ceil(ideal_batch_size_float)
    FILES_PER_BATCH = max(1, min(SAFE_MAX_BATCH_SIZE, ideal_batch_size))
    batch_size_source = f"dynamic (capped at {SAFE_MAX_BATCH_SIZE})"

num_batches = math.ceil(total_files / FILES_PER_BATCH)
# --- End Configuration ---

print(f"--- Read Avro Benchmark ---")
print(f"Total files: {total_files}")
print(f"Files per batch: {FILES_PER_BATCH} ({batch_size_source})")
print(f"Number of batches: {num_batches}")
print(f"Max workers (Pool): {MAX_WORKERS}")
print(f"Threads per worker (DB): {THREADS_PER_WORKER}")
print(f"Memory per worker (DB): {MEMORY_PER_WORKER}")

# Prepare the SQL template once
# Use count(*) to force reading without returning all data
query_template = "SELECT count(*) FROM read_avro([{files}], filename=true);"


def process_batch(batch_index):
    # Create a new connection for each thread
    batch_con = duckdb.connect()

    # Configure connection (No S3 needed)
    batch_con.execute(f"SET memory_limit='{MEMORY_PER_WORKER}';")
    batch_con.execute(f"SET threads TO {THREADS_PER_WORKER};")
    # Set temp directory for potential spilling during read/parse
    batch_con.execute("SET temp_directory = '/tmp/duckdb_read_spill';")

    start_index = batch_index * FILES_PER_BATCH
    end_index = min(start_index + FILES_PER_BATCH, total_files)
    batch_files = all_files[start_index:end_index]
    
    # Handle case where batch_files might be empty if total_files isn't multiple of FILES_PER_BATCH
    if not batch_files:
        print(f"Batch {batch_index} is empty, skipping.")
        batch_con.close()
        return batch_index, 0 # Return 0 count for empty batch

    batch_files_str = ",".join(f"'{file}'" for file in batch_files)
    
    q = query_template.format(files=batch_files_str)
    batch_start_time = time.time()
    result = batch_con.execute(q).fetchone() # Execute and fetch the count
    batch_end_time = time.time()
    
    duration = batch_end_time - batch_start_time
    count = result[0] if result else 0
    
    print(f"Batch {batch_index} read {count} records in {duration:.4f} seconds")
    batch_con.close()
    return batch_index, duration # Return index and duration

# --- Main Execution ---
total_start_time = time.time()
batch_durations = {}

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_batch, i) for i in range(num_batches)]
    for future in concurrent.futures.as_completed(futures):
        try:
            batch_idx, duration = future.result()
            batch_durations[batch_idx] = duration
            print(f"Completed read batch {batch_idx}")
        except Exception as e:
            # Find which batch index caused the error if possible
            # This part might be tricky as future.result() raises the exception directly
            print(f"An error occurred in one of the batches: {e}")
            # Attempt to find which future failed (less reliable)
            failed_future_indices = [i for i, f in enumerate(futures) if f == future]
            if failed_future_indices:
                 print(f"Error likely occurred in processing batch for index: {failed_future_indices[0]}") # Assuming only one future matches

total_end_time = time.time()
total_duration = total_end_time - total_start_time

print(f"--- Benchmark Summary ---")
print(f"Total execution time: {total_duration:.4f} seconds")

# Optional: Print individual batch times
#for i in sorted(batch_durations.keys()):
#    print(f"Batch {i} duration: {batch_durations[i]:.4f} seconds")

avg_batch_time = sum(batch_durations.values()) / len(batch_durations) if batch_durations else 0
print(f"Average batch processing time: {avg_batch_time:.4f} seconds")
print(f"--- End Benchmark ---") 