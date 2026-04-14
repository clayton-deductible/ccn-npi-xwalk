"""
CLI entrypoint for ccn-npi-xwalk.

Usage:
    ccn-npi-xwalk get [--output PATH]
    ccn-npi-xwalk info
"""

import sys
import argparse
from ccn_npi_xwalk.download import download_csv, get_latest_release_info, ASSET_NAME


def cmd_get(args):
    output = args.output or ASSET_NAME
    try:
        download_csv(output_path=output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_info(args):
    try:
        tag, published, url, size_bytes = get_latest_release_info()
        print(f"Latest release: {tag}")
        print(f"Published:      {published}")
        print(f"Size:           {size_bytes / 1024 / 1024:.1f} MB")
        print(f"Download URL:   {url}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        prog="ccn-npi-xwalk",
        description="Download the CCN→NPI crosswalk for CMS hospital facilities.",
    )
    subparsers = parser.add_subparsers(dest="command")

    get_parser = subparsers.add_parser(
        "get",
        help="Download the latest crosswalk CSV to your current directory.",
    )
    get_parser.add_argument(
        "--output",
        metavar="PATH",
        default=None,
        help=f"Output file path (default: ./{ASSET_NAME})",
    )

    subparsers.add_parser(
        "info",
        help="Show metadata for the latest release without downloading.",
    )

    args = parser.parse_args()

    if args.command == "get":
        cmd_get(args)
    elif args.command == "info":
        cmd_info(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
