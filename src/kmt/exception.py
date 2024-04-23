"""
Contains common exceptions used by kmt
"""

class PipelineRunException(Exception):
    """
    Exception representing a runtime error while processing the pipeline
    """
    pass

class PipelineConfigException(Exception):
    """
    Exception representing a config error while processing the pipeline
    """
    pass

class ValidationException(Exception):
    """
    Validation of some condition failed
    """

class RecursionLimitException(Exception):
    """
    The depth limit for recursion was exceeded
    """

class KMTInternalException(Exception):
    """
    KMT internal error
    """

class KMTConversionException(Exception):
    """
    Error performing type conversion
    """

class KMTResolveException(Exception):
    """
    Error resolving variable references
    """

class KMTManifestException(Exception):
    """
    Error in the structure or content of the manifest
    """

class KMTUnimplementedException(Exception):
    """
    Functionality has not been implemented
    """

class KMTTemplateException(Exception):
    """
    Error performing templating
    """

class KMTConfigException(Exception):
    """
    Error in the pipeline configuration
    """
