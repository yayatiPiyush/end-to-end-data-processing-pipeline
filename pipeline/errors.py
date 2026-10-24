"""Exception hierarchy.

Each class maps to a distinct process exit code so a scheduler (cron, Airflow,
CI) can tell "bad input, page the data owner" apart from "bug, page the
engineer" without parsing logs.
"""


class PipelineError(Exception):
    """Base class for expected, handled pipeline failures."""

    exit_code = 1


class ConfigError(PipelineError):
    """The configuration file is missing, malformed or inconsistent."""

    exit_code = 2


class IngestionError(PipelineError):
    """A source cannot be read at all (missing, empty, wrong schema, bad encoding)."""

    exit_code = 2


class DataQualityError(PipelineError):
    """Input was readable, but too much of it failed validation to load safely."""

    exit_code = 3


class IntegrityError(PipelineError):
    """A post-load consistency check failed; the transaction was rolled back."""

    exit_code = 4
