# src/ateda_platform/partitions.py
from dagster import DailyPartitionsDefinition

# Define reusable partition definitions here
daily_partitions = DailyPartitionsDefinition(start_date="2018-06-01")
