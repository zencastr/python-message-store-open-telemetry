# message-store-open-telemetry

OpenTelemetry distributed tracing for the [`message-store`](https://github.com/zencastr/message-store)
package (NATS JetStream event sourcing).

It wraps a message store and layers tracing on top of its public interface — the same interception
pattern used in the JavaScript version. No tracing code lives in the message store core; all
OpenTelemetry concerns are contained here.

- **Publish** → a `PRODUCER` span is started and W3C trace context is injected into the NATS message
  **headers** (not the event body, so application schemas stay clean).
- **Consume** → each handler runs inside a `CONSUMER` span whose parent is extracted from those
  headers, joining the same trace across the service hop. A backdated `queue_wait` span captures how
  long the message sat in the stream before delivery.
- `ensure_stream`, `fetch` and `wait_for` pass straight through, untouched.

## Installation

```sh
pip install message-store-open-telemetry
# or: uv add message-store-open-telemetry
```

Depends on `message-store>=0.4.0` (the version that exposes `_raw_jetstream_message` and the publish
`headers` parameter this package relies on) and `opentelemetry-api`. `nats-py` arrives transitively
through `message-store`.

## Usage

Wrap the store immediately after building it:

```python
import nats
from message_store import MessageStore
from message_store_open_telemetry import get_message_store_with_open_telemetry

nats_connection = await nats.connect("nats://127.0.0.1:4222")

message_store = get_message_store_with_open_telemetry(
    MessageStore(
        nats_connection,
        prefix="my-service",
        should_create_missing_streams=True,
    )
)

# Use `message_store` exactly as before — publishing and subscriptions are now traced.
```

The wrapper exposes the same surface as `MessageStore`. Type code that accepts either the concrete
store or a wrapped one against `MessageStoreProtocol` (exported from `message_store`):

```python
from message_store import MessageStoreProtocol

def register(store: MessageStoreProtocol) -> None:
    ...
```

## Enabling the SDK

This library only uses the OpenTelemetry **API**. Spans are emitted only once the host process
registers an OpenTelemetry **SDK** and tracer provider. With no SDK registered, every API call is a
cheap no-op, so the wrapper is safe to apply in services that are not exporting traces yet.

A minimal SDK setup (configure the exporter for your collector):

```python
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

provider = TracerProvider(resource=Resource.create({"service.name": "my-service"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)
```

## Emitted spans and attributes

**Publish — `nats publish <subject>` (`PRODUCER`)**

| Attribute                     | Source                     |
| ----------------------------- | -------------------------- |
| `messaging.system`            | `"nats"`                   |
| `messaging.operation`         | `"publish"`                |
| `messaging.destination.name`  | published subject          |
| `messaging.message.body.type` | `message.type`             |
| `messaging.nats.stream`       | from the returned `PubAck` |
| `messaging.message.seq`       | from the returned `PubAck` |

**Consume — `nats process <subject>` (`CONSUMER`)**

Parented to (and linked to) the producer span when trace headers are present.

| Attribute                         | Source                  |
| --------------------------------- | ----------------------- |
| `messaging.system`                | `"nats"`                |
| `messaging.operation`             | `"process"`             |
| `messaging.destination.name`      | message subject         |
| `messaging.message.seq`           | message sequence        |
| `messaging.message.body.type`     | `message.type`          |
| `messaging.nats.consumer`         | consumer (durable) name |
| `messaging.nats.stream`           | raw JetStream message   |
| `messaging.nats.redelivery_count` | raw JetStream message   |

**`queue_wait`** — a child span backdated to the moment JetStream stored the message and ended at
delivery, so its duration is the stream dwell time (tagged with `messaging.nats.dwell_ms` and
`messaging.nats.redelivery_count`).

> Handler errors are recorded on the consumer span and re-raised, so the message store core keeps
> full ownership of `ack`/`nak`/`term`.

## How context propagates

The publish seam injects the active context into a NATS headers dict via `inject`; the consume seam
reads them back via `extract`. Whatever propagator your SDK installs (W3C `tracecontext` by default)
is honoured — no propagator is forced here. Trace context never touches the persisted event payload.

## Exports

- `get_message_store_with_open_telemetry(store: MessageStoreProtocol) -> MessageStoreProtocol`
- `tracer` — the library's `Tracer` instance, exported for advanced use.
