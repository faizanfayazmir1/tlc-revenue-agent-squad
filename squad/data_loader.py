"""
Data loader for NYC TLC Yellow Taxi Parquet files and Taxi Zone Lookup CSV.
Fetches automatically from the public NYC TLC CloudFront distribution if a
requested month is not cached locally in `data/`.
"""

from __future__ import annotations

from pathlib import Path
import urllib.error
import urllib.request


TLC_TRIP_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
TLC_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def ensure_zone_lookup(data_dir: Path) -> Path:
    """Ensures taxi_zone_lookup.csv is present in data_dir."""
    data_dir.mkdir(parents=True, exist_ok=True)
    target = data_dir / "taxi_zone_lookup.csv"
    if target.exists() and target.stat().st_size > 0:
        return target
    req = urllib.request.Request(
        TLC_ZONE_LOOKUP_URL,
        headers={"User-Agent": "Spyne-Revenue-Squad/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp, target.open("wb") as out:
        out.write(resp.read())
    return target


def ensure_month_parquet(month: str, data_dir: Path, force_download: bool = False) -> Path:
    """
    Ensures yellow_tripdata_{month}.parquet exists in data_dir.
    If not present locally (or if force_download=True), downloads it from
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    target = data_dir / f"yellow_tripdata_{month}.parquet"
    if target.exists() and target.stat().st_size > 0 and not force_download:
        return target

    url = TLC_TRIP_URL_TEMPLATE.format(month=month)
    print(f"[DATA-LOADER] Downloading NYC TLC Parquet for {month} from {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "Spyne-Revenue-Squad/1.0"})
    tmp_path = target.with_suffix(".parquet.part")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp, tmp_path.open("wb") as out:
            while True:
                chunk = resp.read(1024 * 64)
                if not chunk:
                    break
                out.write(chunk)
        tmp_path.replace(target)
        print(f"[DATA-LOADER] Saved {target} ({target.stat().st_size / (1024 * 1024):.2f} MB)")
        return target
    except urllib.error.URLError as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise FileNotFoundError(
            f"Parquet file for month '{month}' not found locally at {target} "
            f"and download from {url} failed: {exc}"
        ) from exc
