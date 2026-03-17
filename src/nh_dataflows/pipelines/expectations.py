"""Shared DLT data quality expectations for the traffic pipeline."""

# Bronze expectations — minimal, just ensure records are parseable
BRONZE_EXPECTATIONS = {
    "valid_site_id": "site_id IS NOT NULL",
    "valid_report_date": "report_date IS NOT NULL",
}

# Silver expectations — data quality rules
SILVER_EXPECTATIONS = {
    "valid_speed": "avg_speed IS NULL OR (avg_speed >= 0 AND avg_speed <= 120)",
    "valid_volume": "total_volume IS NULL OR total_volume >= 0",
    "has_coordinates": "latitude IS NOT NULL AND longitude IS NOT NULL",
    "valid_latitude": "latitude BETWEEN 49.0 AND 61.0",
    "valid_longitude": "longitude BETWEEN -8.0 AND 2.0",
}

# Gold expectations — completeness checks
GOLD_EXPECTATIONS = {
    "has_h3_index": "h3_index_res7 IS NOT NULL",
    "has_road_name": "road_name IS NOT NULL AND road_name != ''",
}
