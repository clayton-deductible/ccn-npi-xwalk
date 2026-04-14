"""
Download the latest pre-built CCN→NPI crosswalk CSV from GitHub Releases.
"""

import os
import sys
import requests

GITHUB_API = "https://api.github.com/repos/clayton-deductible/ccn-npi-xwalk/releases/latest"
ASSET_NAME = "ccn_npi_crosswalk.csv"


def get_latest_release_info():
    resp = requests.get(GITHUB_API, timeout=10)
    resp.raise_for_status()
    release = resp.json()
    tag = release.get("tag_name", "unknown")
    published = release.get("published_at", "")[:10]
    assets = release.get("assets", [])
    for asset in assets:
        if asset["name"] == ASSET_NAME:
            return tag, published, asset["browser_download_url"], asset["size"]
    raise RuntimeError(
        f"Asset '{ASSET_NAME}' not found in latest release ({tag}). "
        "Check https://github.com/clayton-deductible/ccn-npi-xwalk/releases"
    )


def download_csv(output_path: str = None, show_progress: bool = True) -> str:
    tag, published, url, size_bytes = get_latest_release_info()

    if output_path is None:
        output_path = ASSET_NAME

    if show_progress:
        print(f"Release:   {tag} (published {published})")
        print(f"Size:      {size_bytes / 1024 / 1024:.1f} MB")
        print(f"Saving to: {output_path}")
        print()

    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    downloaded = 0
    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if show_progress:
                    pct = downloaded * 100 // size_bytes
                    bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
                    print(f"\r  [{bar}] {pct}%", end="", flush=True)

    if show_progress:
        print(f"\r  [####################] 100%")
        print(f"\nDone. {downloaded / 1024 / 1024:.1f} MB written to {output_path}")

    return output_path
