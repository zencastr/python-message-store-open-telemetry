import asyncio
import functools
import time
from typing import Callable, Dict, Optional

from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import Link, SpanKind

from message_store import Message, MessageFromSubscription, MessageStoreProtocol

from .tracing import replace_subject_ids_with_placeholder, tracer


def get_message_store_with_open_telemetry(
    message_store: MessageStoreProtocol,
) -> MessageStoreProtocol:
    """
    Wraps an existing message store and layers OpenTelemetry tracing on top of its public
    interface, mirroring the zen-log interception pattern. All OTel concerns live here; the core
    message store stays dependency-free.

    - publish:  starts a PRODUCER span and injects W3C trace context into NATS headers.
    - consume:  wraps each handler in a CONSUMER span whose parent is extracted from the incoming
                message's headers, plus a backdated ``queue_wait`` span for stream dwell.

    ensure_stream / fetch / wait_for (and any future methods) pass straight through.
    """
    return _OpenTelemetryMessageStore(message_store)


class _OpenTelemetryMessageStore:
    def __init__(self, store: MessageStoreProtocol):
        self._store = store

    async def publish_message(
        self,
        subject: str,
        message: Message,
        msg_id: Optional[str] = None,
        timeout_in_seconds: Optional[float] = 60,
        headers: Optional[Dict[str, str]] = None,
    ):
        # PRODUCER span as a child of whatever context is active (e.g. an HTTP server span, or a
        # consumer process span when re-publishing from a handler — the cross-service hop).
        with tracer.start_as_current_span(
            f"nats publish {replace_subject_ids_with_placeholder(subject)}",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "nats",
                "messaging.operation": "publish",
                "messaging.destination.name": subject,
                "messaging.message.body.type": message.type,
            },
        ) as span:
            # Carry caller-provided headers through, then inject the trace context on top.
            carrier: Dict[str, str] = dict(headers) if headers else {}
            inject(carrier)
            ack = await self._store.publish_message(
                subject,
                message,
                msg_id=msg_id,
                timeout_in_seconds=timeout_in_seconds,
                headers=carrier,
            )
            span.set_attribute("messaging.nats.stream", ack.stream)
            span.set_attribute("messaging.message.seq", ack.seq)
            return ack

    def create_subscription(
        self,
        subject: str,
        consumer_name: str,
        handlers: Dict[str, Callable[[MessageFromSubscription], None]],
        max_number_of_retries: int = 3,
        dead_letter_subject: Optional[str] = None,
    ):
        traced_handlers = {
            message_type: self._trace_handler(handler, subject, consumer_name)
            for message_type, handler in handlers.items()
        }
        return self._store.create_subscription(
            subject,
            consumer_name,
            traced_handlers,
            max_number_of_retries,
            dead_letter_subject,
        )

    def _trace_handler(
        self,
        handler: Callable[[MessageFromSubscription], None],
        subject: str,
        consumer_name: str,
    ) -> Callable[[MessageFromSubscription], None]:
        @functools.wraps(handler)
        async def traced(message: MessageFromSubscription):
            raw = getattr(message, "_raw_jetstream_message", None)

            # Extract the producer context the publish seam injected, so the consumer span joins the
            # same trace. We make it the parent (single waterfall for the common 1:1 case) AND link
            # to it (the convention every consumer inherits; what a future fan-out consumer relies on).
            carrier = (raw.headers if raw is not None else None) or {}
            parent_context = extract(carrier)
            parent_span_context = trace.get_current_span(parent_context).get_span_context()
            links = [Link(parent_span_context)] if parent_span_context.is_valid else []

            attributes = {
                "messaging.system": "nats",
                "messaging.operation": "process",
                "messaging.destination.name": message.subject,
                "messaging.message.seq": message.seq,
                "messaging.message.body.type": message.type,
                "messaging.nats.consumer": consumer_name,
            }
            if raw is not None:
                attributes["messaging.nats.stream"] = raw.metadata.stream
                attributes["messaging.nats.redelivery_count"] = raw.metadata.num_delivered
            if message.metadata is not None:
                # Origin of the causation chain the app already tracks in the body metadata; handy
                # for correlating a trace back to the producing subject. Always camelCase.
                if message.metadata.originSubject is not None:
                    attributes["messaging.message.body.metadata.originSubject"] = (
                        message.metadata.originSubject
                    )
                # zen-log trace id, captured whichever way the producer stamped it: the Python
                # zen_log sets a first-class metadata.traceId, while the JS zen-log nests it under a
                # __zenlog object (which lands in additional_props here). Only set when present.
                if message.metadata.traceId is not None:
                    attributes["messaging.message.body.metadata.traceId"] = message.metadata.traceId
                zenlog = message.metadata.additional_props.get("__zenlog")
                if isinstance(zenlog, dict) and zenlog.get("traceId") is not None:
                    attributes["messaging.message.body.metadata.__zenlog.traceId"] = zenlog["traceId"]

            span = tracer.start_span(
                f"nats process {subject} ({consumer_name})",
                context=parent_context,
                kind=SpanKind.CONSUMER,
                links=links,
                attributes=attributes,
            )

            # use_span activates the consumer span, records exceptions, sets ERROR status, and ends
            # the span on exit — then re-raises, so the core's loop still performs nak/term.
            with trace.use_span(
                span, end_on_exit=True, record_exception=True, set_status_on_exception=True
            ):
                if raw is not None:
                    # Backdated queue-dwell span: startTime = the moment JetStream stored the
                    # message, ended now (delivery), so its duration IS the dwell. redelivery_count
                    # tags it because dwell on a redelivery includes nak/backoff, not pure wait.
                    stored_at = raw.metadata.timestamp.timestamp()
                    queue_wait_span = tracer.start_span(
                        "queue_wait",
                        start_time=int(stored_at * 1e9),
                        attributes={
                            "messaging.system": "nats",
                            "messaging.nats.stream": raw.metadata.stream,
                            "messaging.nats.consumer": consumer_name,
                            "messaging.nats.dwell_ms": (time.time() - stored_at) * 1000,
                            "messaging.nats.redelivery_count": raw.metadata.num_delivered,
                        },
                    )
                    queue_wait_span.end()

                if asyncio.iscoroutinefunction(handler):
                    await handler(message)
                else:
                    handler(message)

        return traced

    def __getattr__(self, name):
        # Delegate everything not overridden above (ensure_stream, fetch, wait_for, and any future
        # methods) to the wrapped store — the Python analog of the JS .bind pass-throughs.
        return getattr(self._store, name)
