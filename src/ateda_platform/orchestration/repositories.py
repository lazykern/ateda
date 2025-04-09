# src/ateda_platform/orchestration/repositories.py
from dagster import repository
from .assets.hello_world_asset import hello_world_asset

@repository
def ateda_platform_repo():
    """
    Defines the ATEDA platform repository containing assets, jobs, etc.
    """
    # Collect all definitions (assets, jobs, schedules, sensors) here
    return [hello_world_asset]
