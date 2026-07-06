import asyncio
import unittest
from datetime import datetime, timezone

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from message_store import MessageFromSubscription, MessageMetadata
from message_store_open_telemetry import get_message_store_with_open_telemetry

# One global provider + in-memory exporter for the whole module. get_tracer returns a proxy that
# resolves this provider lazily at span-creation time, so registering it here (after the wrapper was
# imported) is fine. set_tracer_provider only takes effect once per process.
_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


class _CapturingStore:
    """Minimal MessageStore stand-in that records the (traced) handlers it is given."""

    def __init__(self):
        self.captured_handlers = {}

    def create_subscription(
        self, subject, consumer_name, handlers, max_number_of_retries, dead_letter_subject
    ):
        self.captured_handlers = handlers
        return None


def _run_consume(subject: str, consumer_name: str, message: MessageFromSubscription):
    """Wrap a store, register a handler, and drive one message through the traced handler."""
    store = _CapturingStore()
    wrapped = get_message_store_with_open_telemetry(store)

    async def handler(_message):
        return None

    wrapped.create_subscription(subject, consumer_name, {message.type: handler})
    asyncio.run(store.captured_handlers[message.type](message))


class ConsumerSpanTest(unittest.TestCase):
    def setUp(self):
        _exporter.clear()

    def _only_span(self):
        # No _raw_jetstream_message on the test messages, so no queue_wait span — exactly one span.
        spans = _exporter.get_finished_spans()
        self.assertEqual(len(spans), 1, [s.name for s in spans])
        return spans[0]

    def _message(self, metadata):
        return MessageFromSubscription(
            type="VideoClipCreated",
            data={},
            seq=1,
            subject="import-podcast.61dc61d8a5073a4d0b036cc5",
            timestamp=datetime.now(timezone.utc),
            metadata=metadata,
        )

    def test_span_named_by_subscription_subject_and_consumer(self):
        _run_consume("import-podcast.>", "my-consumer", self._message(None))
        self.assertEqual(self._only_span().name, "nats process import-podcast.> (my-consumer)")

    def test_origin_subject_and_python_first_class_trace_id(self):
        metadata = MessageMetadata(origin_subject="clip:command", trace_id="py-trace-123")
        _run_consume("import-podcast.>", "my-consumer", self._message(metadata))
        attrs = self._only_span().attributes
        self.assertEqual(attrs["messaging.message.body.metadata.originSubject"], "clip:command")
        self.assertEqual(attrs["messaging.message.body.metadata.traceId"], "py-trace-123")
        self.assertNotIn("messaging.message.body.metadata.__zenlog.traceId", attrs)

    def test_js_nested_zenlog_trace_id(self):
        # JS zen-log stamps a nested __zenlog object; the Python parser lands it in additional_props.
        metadata = MessageMetadata(__zenlog={"traceId": "js-trace-456"})
        _run_consume("import-podcast.>", "my-consumer", self._message(metadata))
        attrs = self._only_span().attributes
        self.assertEqual(attrs["messaging.message.body.metadata.__zenlog.traceId"], "js-trace-456")
        self.assertNotIn("messaging.message.body.metadata.traceId", attrs)

    def test_no_metadata_sets_no_metadata_attributes(self):
        _run_consume("import-podcast.>", "my-consumer", self._message(None))
        attrs = self._only_span().attributes
        self.assertNotIn("messaging.message.body.metadata.originSubject", attrs)
        self.assertNotIn("messaging.message.body.metadata.traceId", attrs)
        self.assertNotIn("messaging.message.body.metadata.__zenlog.traceId", attrs)


if __name__ == "__main__":
    unittest.main()
