from luigi import build
import os
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--full")
    args = parser.parse_args()
    subset = True
    if args.full:
        subset = False

    build(
        [ByStars(subset=subset), ByDecade(subset=subset)],
        local_scheduler=True,
        log_level="INFO",
    )
