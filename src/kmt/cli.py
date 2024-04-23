
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

def main():
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

    except BrokenPipeError as e:
        try:
            print("Broken Pipe", file=sys.stderr)
            if not sys.stderr.closed:
                sys.stderr.close()
        except:
            pass

        sys.exit(1)

    except Exception as e:  # pylint: disable=broad-exception-caught
        if debug:
            logger.error(e, exc_info=True, stack_info=True)
        else:
            logger.error(e)

        sys.exit(1)

    try:
        sys.stdout.flush()
    except Exception as e:
        sys.exit(1)

    sys.exit(0)
