#!/usr/bin/env python3
"""
"""

import argparse
import logging
import sys

from .exception import *
from .util import *
import ttast
from . import handlers

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
    parser.add_argument(
        "-c", action="append", dest="configs", help="Configuration files"
    )

    parser.add_argument(
        "-d", action="store_true", dest="debug", help="Enable debug output"
    )

    args = parser.parse_args()

    # Capture argument options
    debug = args.debug
    configs = args.configs

    # Logging configuration
    level = logging.WARNING
    if debug:
        level = logging.DEBUG

    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    try:
        # Add each config as a pipeline step, which will read and merge the config
        steps = list()
        if configs is not None:
            for config_item in configs:
                # If '-' is specified, read the configuration from stdin
                if config_item == "-":
                    step_def = {
                        "type": "config",
                        "stdin": True
                    }
                else:
                    step_def = {
                        "type": "config",
                        "file": config_item
                    }

                steps.append(step_def)

        pipeline = ttast.Pipeline()

        # Include the default handlers and support handlers
        pipeline.add_handlers(ttast.builtin_handlers())
        pipeline.add_support_handlers(ttast.builtin_support_handlers())

        # Add our custom handlers and support handlers
        pipeline.add_support_handlers([
            handlers.SupportHandlerSplitYaml,
            handlers.SupportHandlerExtractMetadata,
            handlers.SupportHandlerStoreParsed
        ])

        pipeline.add_handlers({
            "metadata": handlers.HandlerMetadata
        })

        # Add the steps we've defined to the pipeline
        for step in steps:
            pipeline.add_step(step)

        # Start executing the pipeline
        pipeline.run()

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
