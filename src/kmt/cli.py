
import argparse
import logging
import sys

import kmt.exception as exception
import kmt.util as util
import kmt.core as core
import kmt.step_handlers as step_handlers
import kmt.step_support as step_support
import kmt.pipeline_support as pipeline_support
import kmt.j2support as j2support

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
        pipeline = core.Pipeline(path)

        # Start executing the pipeline
        manifests = pipeline.run()

        logger.debug(f"Received {len(manifests)} manifests from the pipeline")
        for manifest in manifests:
            print(manifest)

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
