#!/usr/bin/env python3
"""
"""

import argparse
import logging
import sys

from .exception import *
from .util import *

from . import types
from . import handlers
from . import filters
from . import support_handlers

logger = logging.getLogger(__name__)

def process_args() -> int:
    """
    Processes kmt command line arguments, initialises and runs the pipeline to perform text processing
    """

    # Create parser for command line arguments
    parser = argparse.ArgumentParser(
        prog="kmt", description="Kubernetes Manifest Transform", exit_on_error=False
    )

    # Parser configuration
    parser.add_argument("path", help="Pipeline directory path")

    parser.add_argument(
        "-d", action="store_true", dest="debug", help="Enable debug output"
    )

    args = parser.parse_args()

    # Capture argument options
    debug = args.debug
    path = args.path

    # Logging configuration
    level = logging.WARNING
    if debug:
        level = logging.DEBUG

    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    try:
        pipeline = types.Pipeline(path)

        # Start executing the pipeline
        blocks = pipeline.run()

        logger.debug(f"Received {len(blocks)} blocks from the pipeline")
        for block in blocks:
            print(block.text)

    except Exception as e:  # pylint: disable=broad-exception-caught
        if debug:
            logger.error(e, exc_info=True, stack_info=True)
        else:
            logger.error(e)
        return 1

    return 0


def main():
    """
    Entrypoint for the module.
    Minor exception handling is performed, along with return code processing and
    flushing of stdout on program exit.
    """
    try:
        ret = process_args()
        sys.stdout.flush()
        sys.exit(ret)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.getLogger(__name__).exception(e)
        sys.stdout.flush()
        sys.exit(1)

if __name__ == "__main__":
    main()
