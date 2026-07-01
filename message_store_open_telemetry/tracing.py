import re

from opentelemetry import trace

from .__about__ import __version__

# Single tracer for this library. Spans only do anything once the host process registers an
# OpenTelemetry SDK + tracer provider; with no SDK the opentelemetry-api calls are cheap no-ops,
# so the library stays safe for services not yet tracing.
tracer = trace.get_tracer("message-store-open-telemetry", __version__)

_OBJECT_ID = re.compile(r"^[0-9a-f]{24}$", re.IGNORECASE)
_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)


def to_low_cardinality_subject(subject: str) -> str:
    """Replace per-message id tokens in a NATS subject with ``{id}``.

    NATS subjects embed a per-message id (Mongo ObjectId / UUID) as a token, e.g.
    ``import-podcast:command.61dc61d8a5073a4d0b036cc5``. A span name is the metrics
    generator's series key, so an id in the name explodes span-metric cardinality
    (one series per message). Collapsing the variable tokens gives one stable series
    per step; the full subject stays on the span as ``messaging.destination.name``
    for per-trace lookups.
    """
    return ".".join(
        "{id}" if _OBJECT_ID.match(token) or _UUID.match(token) else token
        for token in subject.split(".")
    )
