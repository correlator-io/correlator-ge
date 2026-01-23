#!/usr/bin/env python3
"""Generate sample taxi datasets for Great Expectations testing.

Creates two CSV files:
- taxi_clean.csv: Valid data that passes all expectations
- taxi_dirty.csv: Invalid data with NULLs, negative values, and out-of-range values

Run:
    cd tests/fixtures/scripts
    python generate_sample_data.py

Output:
    ../ge_sample_project/data/taxi_clean.csv
    ../ge_sample_project/data/taxi_dirty.csv
"""

from pathlib import Path


def generate_clean_data() -> str:
    """Generate taxi data that passes all expectations.

    Expectations:
    - vendor_id: not null, in set [1, 2]
    - passenger_count: not null, between 1 and 6
    - fare_amount: between 0 and 500
    - trip_distance: >= 0
    """
    header = "vendor_id,passenger_count,fare_amount,trip_distance,pickup_datetime"
    rows = [
        "1,2,15.50,3.2,2024-01-15 10:30:00",
        "2,1,8.75,1.5,2024-01-15 11:00:00",
        "1,4,22.00,5.8,2024-01-15 11:30:00",
        "2,3,12.25,2.9,2024-01-15 12:00:00",
        "1,1,6.50,1.0,2024-01-15 12:30:00",
        "2,5,35.00,8.2,2024-01-15 13:00:00",
        "1,2,18.75,4.1,2024-01-15 13:30:00",
        "2,6,45.00,10.5,2024-01-15 14:00:00",
        "1,1,9.25,2.0,2024-01-15 14:30:00",
        "2,2,14.00,3.5,2024-01-15 15:00:00",
    ]
    return "\n".join([header, *rows]) + "\n"


def generate_dirty_data() -> str:
    """Generate taxi data that fails multiple expectations.

    Violations:
    - Row 2: vendor_id is NULL (fails not_null, in_set)
    - Row 3: vendor_id is 3 (fails in_set)
    - Row 4: passenger_count is NULL (fails not_null)
    - Row 5: passenger_count is 0 (fails between 1-6)
    - Row 6: passenger_count is 10 (fails between 1-6)
    - Row 7: fare_amount is -5.00 (fails >= 0)
    - Row 8: fare_amount is 750.00 (fails <= 500)
    - Row 9: trip_distance is -1.5 (fails >= 0)
    """
    header = "vendor_id,passenger_count,fare_amount,trip_distance,pickup_datetime"
    rows = [
        "1,2,15.50,3.2,2024-01-15 10:30:00",  # Valid
        ",1,8.75,1.5,2024-01-15 11:00:00",  # NULL vendor_id
        "3,4,22.00,5.8,2024-01-15 11:30:00",  # vendor_id = 3 (invalid)
        "2,,12.25,2.9,2024-01-15 12:00:00",  # NULL passenger_count
        "1,0,6.50,1.0,2024-01-15 12:30:00",  # passenger_count = 0
        "2,10,35.00,8.2,2024-01-15 13:00:00",  # passenger_count = 10
        "1,2,-5.00,4.1,2024-01-15 13:30:00",  # negative fare
        "2,3,750.00,10.5,2024-01-15 14:00:00",  # fare > 500
        "1,1,9.25,-1.5,2024-01-15 14:30:00",  # negative distance
        "2,2,14.00,3.5,2024-01-15 15:00:00",  # Valid
    ]
    return "\n".join([header, *rows]) + "\n"


def main() -> None:
    """Generate sample data files."""
    # Create data directory in ge_sample_project (sibling to scripts/)
    data_dir = Path(__file__).parent.parent / "ge_sample_project" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Write clean data
    clean_path = data_dir / "taxi_clean.csv"
    clean_path.write_text(generate_clean_data())
    print(f"Generated: {clean_path}")

    # Write dirty data
    dirty_path = data_dir / "taxi_dirty.csv"
    dirty_path.write_text(generate_dirty_data())
    print(f"Generated: {dirty_path}")


if __name__ == "__main__":
    main()
