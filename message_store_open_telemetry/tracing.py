from opentelemetry import trace

from .__about__ import __version__

# Single tracer for this library. Spans only do anything once the host process registers an
# OpenTelemetry SDK + tracer provider; with no SDK the opentelemetry-api calls are cheap no-ops,
# so the library stays safe for services not yet tracing.
tracer = trace.get_tracer("message-store-open-telemetry", __version__)
