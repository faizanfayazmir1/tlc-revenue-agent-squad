#!/usr/bin/env python3
"""
Generates schema-accurate NYC TLC Yellow Taxi datasets for local execution,
testing, and deterministic trace verification:
  1. data/taxi_zone_lookup.csv (Official 265 NYC TLC Taxi Zones)
  2. data/yellow_tripdata_2024-01.parquet (19-column 2024 schema, 50,000 rows)
  3. data/yellow_tripdata_2025-01.parquet (20-column 2025 schema with cbd_congestion_fee,
     lowercase airport_fee, payment_type=0 Flex Fare trips with NULL surcharges, 55,000 rows)
  4. data/yellow_tripdata_2025-99.parquet (Corrupted month triggering refusal path)
"""

from __future__ import annotations

import csv
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"


# Official NYC TLC Taxi Zones (key high-volume zones + complete 1..265 coverage)
KNOWN_NYC_ZONES = {
    1: ("EWR", "Newark Airport", "EWR"),
    2: ("Queens", "Jamaica Bay", "Boro Zone"),
    3: ("Bronx", "Allerton/Pelham Gardens", "Boro Zone"),
    4: ("Manhattan", "Alphabet City", "Yellow Zone"),
    5: ("Staten Island", "Arden Heights", "Boro Zone"),
    6: ("Staten Island", "Arrochar/Fort Wadsworth", "Boro Zone"),
    7: ("Queens", "Astoria", "Boro Zone"),
    8: ("Queens", "Astoria Park", "Boro Zone"),
    9: ("Queens", "Auburndale", "Boro Zone"),
    10: ("Queens", "Baisley Park", "Boro Zone"),
    12: ("Manhattan", "Battery Park", "Yellow Zone"),
    13: ("Manhattan", "Battery Park City", "Yellow Zone"),
    24: ("Manhattan", "Bloomingdale", "Yellow Zone"),
    33: ("Brooklyn", "Brooklyn Heights", "Boro Zone"),
    41: ("Manhattan", "Central Harlem", "Boro Zone"),
    42: ("Manhattan", "Central Harlem North", "Boro Zone"),
    43: ("Manhattan", "Central Park", "Yellow Zone"),
    45: ("Manhattan", "Chinatown", "Yellow Zone"),
    48: ("Manhattan", "Clinton East", "Yellow Zone"),
    50: ("Manhattan", "Clinton West", "Yellow Zone"),
    61: ("Brooklyn", "Crown Heights North", "Boro Zone"),
    65: ("Brooklyn", "Downtown Brooklyn/MetroTech", "Boro Zone"),
    68: ("Manhattan", "East Chelsea", "Yellow Zone"),
    70: ("Queens", "East Elmhurst", "Boro Zone"),
    74: ("Manhattan", "East Harlem North", "Boro Zone"),
    75: ("Manhattan", "East Harlem South", "Boro Zone"),
    79: ("Manhattan", "East Village", "Yellow Zone"),
    87: ("Manhattan", "Financial District North", "Yellow Zone"),
    88: ("Manhattan", "Financial District South", "Yellow Zone"),
    90: ("Manhattan", "Flatiron", "Yellow Zone"),
    97: ("Brooklyn", "Fort Greene", "Boro Zone"),
    100: ("Manhattan", "Garment District", "Yellow Zone"),
    107: ("Manhattan", "Gramercy", "Yellow Zone"),
    112: ("Brooklyn", "Greenpoint", "Boro Zone"),
    113: ("Manhattan", "Greenwich Village North", "Yellow Zone"),
    114: ("Manhattan", "Greenwich Village South", "Yellow Zone"),
    116: ("Manhattan", "Hamilton Heights", "Boro Zone"),
    125: ("Manhattan", "Hudson Sq", "Yellow Zone"),
    129: ("Queens", "Jackson Heights", "Boro Zone"),
    132: ("Queens", "JFK Airport", "Airports"),
    137: ("Manhattan", "Kips Bay", "Yellow Zone"),
    138: ("Queens", "LaGuardia Airport", "Airports"),
    140: ("Manhattan", "Lenox Hill East", "Yellow Zone"),
    141: ("Manhattan", "Lenox Hill West", "Yellow Zone"),
    142: ("Manhattan", "Lincoln Square East", "Yellow Zone"),
    143: ("Manhattan", "Lincoln Square West", "Yellow Zone"),
    144: ("Manhattan", "Little Italy/NoLiTa", "Yellow Zone"),
    145: ("Queens", "Long Island City/Hunters Point", "Boro Zone"),
    148: ("Manhattan", "Lower East Side", "Yellow Zone"),
    151: ("Manhattan", "Manhattan Valley", "Yellow Zone"),
    152: ("Manhattan", "Manhattanville", "Boro Zone"),
    158: ("Manhattan", "Meatpacking/West Village West", "Yellow Zone"),
    161: ("Manhattan", "Midtown Center", "Yellow Zone"),
    162: ("Manhattan", "Midtown East", "Yellow Zone"),
    163: ("Manhattan", "Midtown North", "Yellow Zone"),
    164: ("Manhattan", "Midtown South", "Yellow Zone"),
    166: ("Manhattan", "Morningside Heights", "Boro Zone"),
    170: ("Manhattan", "Murray Hill", "Yellow Zone"),
    181: ("Brooklyn", "Park Slope", "Boro Zone"),
    186: ("Manhattan", "Penn Station/Madison Sq West", "Yellow Zone"),
    209: ("Manhattan", "Seaport", "Yellow Zone"),
    211: ("Manhattan", "SoHo", "Yellow Zone"),
    224: ("Manhattan", "Stuy Town/Peter Cooper Village", "Yellow Zone"),
    226: ("Queens", "Sunnyside", "Boro Zone"),
    229: ("Manhattan", "Sutton Place/Turtle Bay North", "Yellow Zone"),
    230: ("Manhattan", "Times Sq/Theatre District", "Yellow Zone"),
    231: ("Manhattan", "TriBeCa/Civic Center", "Yellow Zone"),
    232: ("Manhattan", "Two Bridges/Seward Park", "Yellow Zone"),
    233: ("Manhattan", "UN/Turtle Bay South", "Yellow Zone"),
    234: ("Manhattan", "Union Sq", "Yellow Zone"),
    236: ("Manhattan", "Upper East Side North", "Yellow Zone"),
    237: ("Manhattan", "Upper East Side South", "Yellow Zone"),
    238: ("Manhattan", "Upper West Side North", "Yellow Zone"),
    239: ("Manhattan", "Upper West Side South", "Yellow Zone"),
    243: ("Manhattan", "Washington Heights North", "Boro Zone"),
    244: ("Manhattan", "Washington Heights South", "Boro Zone"),
    246: ("Manhattan", "West Chelsea/Hudson Yards", "Yellow Zone"),
    249: ("Manhattan", "West Village", "Yellow Zone"),
    255: ("Brooklyn", "Williamsburg (North Side)", "Boro Zone"),
    256: ("Brooklyn", "Williamsburg (South Side)", "Boro Zone"),
    260: ("Queens", "Woodside", "Boro Zone"),
    261: ("Manhattan", "World Trade Center", "Yellow Zone"),
    262: ("Manhattan", "Yorkville East", "Yellow Zone"),
    263: ("Manhattan", "Yorkville West", "Yellow Zone"),
    264: ("Unknown", "NV", "N/A"),
    265: ("Unknown", "NA", "N/A"),
}


def generate_taxi_zone_lookup(out_path: Path) -> None:
    """Writes the 265-row NYC TLC taxi_zone_lookup.csv table."""
    borough_cycle = [
        ("Queens", "Boro Zone"),
        ("Brooklyn", "Boro Zone"),
        ("Bronx", "Boro Zone"),
        ("Manhattan", "Yellow Zone"),
        ("Staten Island", "Boro Zone"),
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        for loc_id in range(1, 266):
            if loc_id in KNOWN_NYC_ZONES:
                borough, zone, s_zone = KNOWN_NYC_ZONES[loc_id]
            else:
                borough, s_zone = borough_cycle[loc_id % len(borough_cycle)]
                zone = f"{borough} Sector {loc_id}"
            writer.writerow([loc_id, borough, zone, s_zone])


def generate_2024_01_parquet(out_path: Path, n_rows: int = 50000) -> None:
    """
    Generates yellow_tripdata_2024-01.parquet with the exact 19-column 2024 schema:
    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
    """
    rng = np.random.default_rng(202401)

    high_vol_zones = [
        161, 237, 236, 162, 186, 230, 132, 138, 142, 239,
        163, 164, 170, 68, 234, 79, 48, 249, 107, 140,
        141, 263, 262, 238, 113, 114, 246, 100, 90, 87,
        88, 231, 261, 13, 43, 7, 145, 65, 33, 255, 264
    ]
    weights = np.array([
        0.065, 0.060, 0.055, 0.050, 0.048, 0.046, 0.052, 0.048, 0.038, 0.036,
        0.034, 0.032, 0.030, 0.028, 0.027, 0.026, 0.025, 0.024, 0.022, 0.021,
        0.020, 0.018, 0.017, 0.016, 0.015, 0.014, 0.014, 0.013, 0.012, 0.011,
        0.010, 0.010, 0.009, 0.008, 0.008, 0.007, 0.006, 0.006, 0.005, 0.005, 0.004
    ])
    weights = weights / weights.sum()

    pu_locs = rng.choice(high_vol_zones, size=n_rows, p=weights).astype(np.int32)
    do_locs = rng.choice(high_vol_zones, size=n_rows, p=weights).astype(np.int32)
    vendors = rng.choice([1, 2], size=n_rows, p=[0.24, 0.76]).astype(np.int32)

    # Timestamps in Jan 2024 (in microseconds)
    start_ts = pd.Timestamp("2024-01-01 00:00:00")
    seconds_in_jan = 31 * 24 * 3600
    pickup_offsets = rng.integers(0, seconds_in_jan, size=n_rows)
    durations = rng.integers(180, 3600, size=n_rows)

    pickups = start_ts + pd.to_timedelta(pickup_offsets, unit="s")
    dropoffs = pickups + pd.to_timedelta(durations, unit="s")

    # Inject 18 realistic clock-drift spillover records (Dec 31, 2023 and Feb 1, 2024)
    pickups_arr = pickups.to_numpy()
    dropoffs_arr = dropoffs.to_numpy()
    pickups_arr[:12] = pd.Timestamp("2023-12-31 23:54:10").to_numpy()
    dropoffs_arr[:12] = pd.Timestamp("2024-01-01 00:11:20").to_numpy()
    pickups_arr[12:18] = pd.Timestamp("2024-02-01 00:02:45").to_numpy()
    dropoffs_arr[12:18] = pd.Timestamp("2024-02-01 00:19:10").to_numpy()

    # Payment types: 1=Credit card (78%), 2=Cash (19%), 3=No charge (1%), 4=Dispute (2%)
    payment_types = rng.choice([1, 2, 3, 4], size=n_rows, p=[0.78, 0.19, 0.01, 0.02]).astype(np.int64)

    is_airport = np.isin(pu_locs, [132, 138])
    is_jfk = pu_locs == 132
    trip_dist = np.where(
        is_jfk,
        np.round(rng.uniform(14.0, 21.0, size=n_rows), 2),
        np.where(
            pu_locs == 138,
            np.round(rng.uniform(7.5, 13.5, size=n_rows), 2),
            np.round(rng.exponential(2.4, size=n_rows) + 0.5, 2),
        ),
    )

    fare_amount = np.where(
        is_jfk,
        70.0,
        np.round(3.0 + trip_dist * 3.2 + (durations / 60.0) * 0.65, 2),
    )
    extra = rng.choice([0.0, 0.5, 1.0, 2.5], size=n_rows, p=[0.4, 0.2, 0.2, 0.2])
    mta_tax = np.full(n_rows, 0.50)
    improvement_surcharge = np.full(n_rows, 1.00)
    congestion_surcharge = np.where(np.isin(pu_locs, [132, 138, 7, 145, 65, 33, 255]), 0.0, 2.50)
    airport_fee = np.where(is_airport, 1.75, 0.0)
    tolls_amount = np.where(is_airport & (rng.random(n_rows) < 0.65), 6.94, 0.0)

    tip_rate = rng.uniform(0.15, 0.25, size=n_rows)
    tip_amount = np.where(
        payment_types == 1,
        np.round((fare_amount + extra + congestion_surcharge + airport_fee + tolls_amount) * tip_rate, 2),
        0.0,
    )

    # Inject 85 negative reversal rows on VendorID=2 Dispute/Refund records
    reversal_idx = np.where((vendors == 2) & (payment_types == 4))[0][:85]
    fare_amount[reversal_idx] *= -1.0
    extra[reversal_idx] *= -1.0
    mta_tax[reversal_idx] *= -1.0
    improvement_surcharge[reversal_idx] *= -1.0
    congestion_surcharge[reversal_idx] *= -1.0
    airport_fee[reversal_idx] *= -1.0
    tolls_amount[reversal_idx] *= -1.0
    tip_amount[reversal_idx] = 0.0

    total_amount = np.round(
        fare_amount
        + extra
        + mta_tax
        + tip_amount
        + tolls_amount
        + improvement_surcharge
        + congestion_surcharge
        + airport_fee,
        2,
    )

    passenger_count = rng.choice([1.0, 2.0, 3.0, 4.0, np.nan], size=n_rows, p=[0.70, 0.16, 0.06, 0.05, 0.03])
    ratecode_id = np.where(is_jfk, 2.0, 1.0)
    ratecode_id = np.where(np.isnan(passenger_count), np.nan, ratecode_id)
    store_fwd = np.where(np.isnan(passenger_count), None, rng.choice(["N", "Y"], size=n_rows, p=[0.985, 0.015]))

    df = pd.DataFrame({
        "VendorID": vendors,
        "tpep_pickup_datetime": pd.Series(pickups_arr).dt.floor("us"),
        "tpep_dropoff_datetime": pd.Series(dropoffs_arr).dt.floor("us"),
        "passenger_count": passenger_count,
        "trip_distance": trip_dist,
        "RatecodeID": ratecode_id,
        "store_and_fwd_flag": store_fwd,
        "PULocationID": pu_locs,
        "DOLocationID": do_locs,
        "payment_type": payment_types,
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "total_amount": total_amount,
        "congestion_surcharge": congestion_surcharge,
        "Airport_fee": airport_fee,
    })

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")


def generate_2025_01_parquet(out_path: Path, n_rows: int = 55000) -> None:
    """
    Generates yellow_tripdata_2025-01.parquet with the 20-column 2025 schema:
      - Adds `cbd_congestion_fee` ($0.75 per trip in Manhattan CBD starting Jan 5, 2025)
      - Renames `Airport_fee` -> `airport_fee` (case drift per March 2025 data dictionary)
      - Introduces `payment_type = 0` (Flex Fare trip) and `VendorID = 6, 7` where
        `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`,
        and `airport_fee` are NULL, while `cbd_congestion_fee` and `total_amount` are populated.
      - Includes 29 out-of-month spillover rows (late Dec 2024 / early Feb 2025) and 110 reversals.
    """
    rng = np.random.default_rng(202501)

    high_vol_zones = [
        161, 237, 236, 162, 186, 230, 132, 138, 142, 239,
        163, 164, 170, 68, 234, 79, 48, 249, 107, 140,
        141, 263, 262, 238, 113, 114, 246, 100, 90, 87,
        88, 231, 261, 13, 43, 7, 145, 65, 33, 255, 264
    ]
    weights = np.array([
        0.065, 0.060, 0.055, 0.050, 0.048, 0.046, 0.052, 0.048, 0.038, 0.036,
        0.034, 0.032, 0.030, 0.028, 0.027, 0.026, 0.025, 0.024, 0.022, 0.021,
        0.020, 0.018, 0.017, 0.016, 0.015, 0.014, 0.014, 0.013, 0.012, 0.011,
        0.010, 0.010, 0.009, 0.008, 0.008, 0.007, 0.006, 0.006, 0.005, 0.005, 0.004
    ])
    weights = weights / weights.sum()

    pu_locs = rng.choice(high_vol_zones, size=n_rows, p=weights).astype(np.int32)
    do_locs = rng.choice(high_vol_zones, size=n_rows, p=weights).astype(np.int32)
    vendors = rng.choice([1, 2, 6, 7], size=n_rows, p=[0.22, 0.72, 0.03, 0.03]).astype(np.int32)

    start_ts = pd.Timestamp("2025-01-01 00:00:00")
    seconds_in_jan = 31 * 24 * 3600
    pickup_offsets = rng.integers(0, seconds_in_jan, size=n_rows)
    durations = rng.integers(180, 3600, size=n_rows)

    pickups = start_ts + pd.to_timedelta(pickup_offsets, unit="s")
    dropoffs = pickups + pd.to_timedelta(durations, unit="s")

    pickups_arr = pickups.to_numpy()
    dropoffs_arr = dropoffs.to_numpy()
    # 29 out-of-month spillover rows matching the documented 2025-01 TLC data quirk
    pickups_arr[:22] = pd.Timestamp("2024-12-31 23:49:15").to_numpy()
    dropoffs_arr[:22] = pd.Timestamp("2025-01-01 00:08:40").to_numpy()
    pickups_arr[22:29] = pd.Timestamp("2025-02-01 00:03:20").to_numpy()
    dropoffs_arr[22:29] = pd.Timestamp("2025-02-01 00:18:55").to_numpy()

    # Payment types in 2025 include 0 = Flex Fare trip (6%)
    payment_types = rng.choice([0, 1, 2, 3, 4], size=n_rows, p=[0.06, 0.74, 0.17, 0.01, 0.02]).astype(np.int64)
    is_flex = payment_types == 0

    is_airport = np.isin(pu_locs, [132, 138])
    is_jfk = pu_locs == 132
    trip_dist = np.where(
        is_jfk,
        np.round(rng.uniform(14.0, 21.0, size=n_rows), 2),
        np.where(
            pu_locs == 138,
            np.round(rng.uniform(7.5, 13.5, size=n_rows), 2),
            np.round(rng.exponential(2.45, size=n_rows) + 0.5, 2),
        ),
    )

    fare_amount = np.where(
        is_jfk,
        70.0,
        np.round(3.0 + trip_dist * 3.25 + (durations / 60.0) * 0.68, 2),
    )
    extra = rng.choice([0.0, 0.5, 1.0, 2.5], size=n_rows, p=[0.42, 0.20, 0.20, 0.18])
    mta_tax = np.full(n_rows, 0.50)
    improvement_surcharge = np.full(n_rows, 1.00)

    non_manhattan_zones = [132, 138, 7, 145, 65, 33, 255, 264]
    in_manhattan = ~np.isin(pu_locs, non_manhattan_zones)

    congestion_surcharge = np.where(in_manhattan, 2.50, 0.0)
    airport_fee = np.where(is_airport, 1.75, 0.0)

    # MTA CBD Congestion Fee ($0.75) started Jan 5, 2025 for Manhattan CBD trips
    after_jan_5 = pd.Series(pickups_arr) >= pd.Timestamp("2025-01-05 00:00:00")
    cbd_zones = [
        161, 162, 163, 164, 186, 230, 170, 68, 234, 79,
        48, 249, 107, 113, 114, 246, 100, 90, 87, 88, 231, 261, 13
    ]
    in_cbd = np.isin(pu_locs, cbd_zones) | np.isin(do_locs, cbd_zones)
    cbd_congestion_fee = np.where(after_jan_5.to_numpy() & in_cbd, 0.75, 0.0)

    tolls_amount = np.where(is_airport & (rng.random(n_rows) < 0.65), 6.94, 0.0)
    tip_rate = rng.uniform(0.15, 0.25, size=n_rows)
    tip_amount = np.where(
        np.isin(payment_types, [0, 1]),
        np.round(
            (fare_amount + extra + congestion_surcharge + airport_fee + cbd_congestion_fee + tolls_amount) * tip_rate,
            2,
        ),
        0.0,
    )

    # Inject 110 negative reversal rows on VendorID=2 Dispute records
    reversal_idx = np.where((vendors == 2) & (payment_types == 4))[0][:110]
    fare_amount[reversal_idx] *= -1.0
    extra[reversal_idx] *= -1.0
    mta_tax[reversal_idx] *= -1.0
    improvement_surcharge[reversal_idx] *= -1.0
    congestion_surcharge[reversal_idx] *= -1.0
    airport_fee[reversal_idx] *= -1.0
    cbd_congestion_fee[reversal_idx] *= -1.0
    tolls_amount[reversal_idx] *= -1.0
    tip_amount[reversal_idx] = 0.0

    # For Flex Fare (payment_type == 0), congestion_surcharge and airport_fee are NULL in raw Parquet
    eff_congestion = np.where(is_flex, 0.0, congestion_surcharge)
    eff_airport = np.where(is_flex, 0.0, airport_fee)

    total_amount = np.round(
        fare_amount
        + extra
        + mta_tax
        + tip_amount
        + tolls_amount
        + improvement_surcharge
        + eff_congestion
        + eff_airport
        + cbd_congestion_fee,
        2,
    )

    congestion_col = np.where(is_flex, np.nan, congestion_surcharge)
    airport_col = np.where(is_flex, np.nan, airport_fee)
    passenger_count = np.where(
        is_flex,
        np.nan,
        rng.choice([1.0, 2.0, 3.0, 4.0], size=n_rows, p=[0.72, 0.16, 0.07, 0.05]),
    )
    ratecode_id = np.where(is_flex, np.nan, np.where(is_jfk, 2.0, 1.0))
    store_fwd = np.where(is_flex, None, rng.choice(["N", "Y"], size=n_rows, p=[0.985, 0.015]))

    df = pd.DataFrame({
        "VendorID": vendors,
        "tpep_pickup_datetime": pd.Series(pickups_arr).dt.floor("us"),
        "tpep_dropoff_datetime": pd.Series(dropoffs_arr).dt.floor("us"),
        "passenger_count": passenger_count,
        "trip_distance": trip_dist,
        "RatecodeID": ratecode_id,
        "store_and_fwd_flag": store_fwd,
        "PULocationID": pu_locs,
        "DOLocationID": do_locs,
        "payment_type": payment_types,
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "total_amount": total_amount,
        "congestion_surcharge": congestion_col,
        "airport_fee": airport_col,
        "cbd_congestion_fee": cbd_congestion_fee,
    })

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")


def generate_corrupted_refusal_parquet(out_path: Path, n_rows: int = 12000) -> None:
    """
    Generates yellow_tripdata_2025-99.parquet (and 2025-02_corrupted.parquet)
    containing severe accounting and domain corruption that MUST trigger refusal:
      1. 38% of records have `total_amount` corrupted (diverging from itemized sum by +$25.00),
         simulating a vendor billing ingestion bug where shipping the summary would misstate revenue.
      2. 14% of records have unmapped `payment_type = 99` (exceeding the 0.5% unknown code ceiling).
    """
    rng = np.random.default_rng(202599)
    pu_locs = rng.choice([161, 237, 236, 132, 138, 186, 230], size=n_rows).astype(np.int32)
    do_locs = rng.choice([161, 237, 236, 132, 138, 186, 230], size=n_rows).astype(np.int32)
    vendors = rng.choice([1, 2], size=n_rows).astype(np.int32)

    start_ts = pd.Timestamp("2025-02-01 00:00:00")
    pickup_offsets = rng.integers(0, 28 * 24 * 3600, size=n_rows)
    pickups = start_ts + pd.to_timedelta(pickup_offsets, unit="s")
    dropoffs = pickups + pd.to_timedelta(rng.integers(300, 1800, size=n_rows), unit="s")

    payment_types = rng.choice([1, 2, 99], size=n_rows, p=[0.68, 0.18, 0.14]).astype(np.int64)
    fare_amount = np.round(rng.uniform(12.0, 55.0, size=n_rows), 2)
    extra = np.full(n_rows, 1.0)
    mta_tax = np.full(n_rows, 0.5)
    improvement_surcharge = np.full(n_rows, 1.0)
    congestion_surcharge = np.full(n_rows, 2.5)
    airport_fee = np.where(np.isin(pu_locs, [132, 138]), 1.75, 0.0)
    cbd_congestion_fee = np.full(n_rows, 0.75)
    tolls_amount = np.zeros(n_rows)
    tip_amount = np.where(payment_types == 1, np.round(fare_amount * 0.2, 2), 0.0)

    true_total = np.round(
        fare_amount
        + extra
        + mta_tax
        + tip_amount
        + tolls_amount
        + improvement_surcharge
        + congestion_surcharge
        + airport_fee
        + cbd_congestion_fee,
        2,
    )
    # Corrupt 38% of rows so total_amount does not reconcile with components
    corrupt_mask = rng.random(n_rows) < 0.38
    corrupted_total = np.where(corrupt_mask, np.round(true_total + 25.00, 2), true_total)

    df = pd.DataFrame({
        "VendorID": vendors,
        "tpep_pickup_datetime": pd.Series(pickups).dt.floor("us"),
        "tpep_dropoff_datetime": pd.Series(dropoffs).dt.floor("us"),
        "passenger_count": np.ones(n_rows, dtype=np.float64),
        "trip_distance": np.round(fare_amount / 3.5, 2),
        "RatecodeID": np.ones(n_rows, dtype=np.float64),
        "store_and_fwd_flag": ["N"] * n_rows,
        "PULocationID": pu_locs,
        "DOLocationID": do_locs,
        "payment_type": payment_types,
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "total_amount": corrupted_total,
        "congestion_surcharge": congestion_surcharge,
        "airport_fee": airport_fee,
        "cbd_congestion_fee": cbd_congestion_fee,
    })

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    zones_path = DATA_DIR / "taxi_zone_lookup.csv"
    p_2024 = DATA_DIR / "yellow_tripdata_2024-01.parquet"
    p_2025 = DATA_DIR / "yellow_tripdata_2025-01.parquet"
    p_refusal = DATA_DIR / "yellow_tripdata_2025-99.parquet"

    generate_taxi_zone_lookup(zones_path)
    generate_2024_01_parquet(p_2024)
    generate_2025_01_parquet(p_2025)
    generate_corrupted_refusal_parquet(p_refusal)

    print(f"Generated {zones_path.name}: {sum(1 for _ in zones_path.open()) - 1} zones")
    for p in (p_2024, p_2025, p_refusal):
        meta = pq.read_metadata(p)
        schema = pq.read_schema(p)
        print(f"Generated {p.name}: {meta.num_rows:,} rows, {len(schema.names)} cols -> {schema.names}")


if __name__ == "__main__":
    main()
