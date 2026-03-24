"""Configuration objects for the data profiler."""

from dataclasses import dataclass


@dataclass
class ProfilerConfig:
    """Runtime configuration for a profiling run."""

    sample_rows: int = 10000
    max_workers: int = 8
    include_histograms: bool = False
    histogram_bins: int = 10
    include_null_counts: bool = True
    profile_empty_tables: bool = True
    fail_fast: bool = False
    resume: bool = True
    output_path: str = "profiles.jsonl"
