from dagster import DailyPartitionsDefinition

# Define daily partitions starting from when ZTF public alerts began
daily_partitions = DailyPartitionsDefinition(start_date="2018-01-01") 