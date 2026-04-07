# Package io.numaproj.numaflowkt.mapper

Kotlin API for implementing Numaflow **map** UDFs.

A map function transforms messages one at a time as they flow through the pipeline.
Each input message produces zero or more output messages (1:N flat-map). The Numaflow
platform delivers messages continuously via streaming -- there are no discrete batch
boundaries. The SDK handles concurrency internally via the Java SDK's Akka actor system,
so each `Mapper.processMessage` call may run on a different thread.

Implement `Mapper` and wire it into a gRPC server with `mapServer`:

```kotlin
import io.numaproj.numaflowkt.mapper.Message
import io.numaproj.numaflowkt.mapper.mapServer

fun main() {
    mapServer {
        mapper { keys, datum ->
            val upper = String(datum.value).uppercase()
            listOf(Message(value = upper.toByteArray(), keys = keys))
        }
    }.run()
}
```

The mapper receives the message keys (used for routing and partitioning) and the `Datum`
payload, and returns a list of output `Message`s. Return `listOf(Message.drop())` to
filter a message out of the pipeline.

`Mapper` is a `fun interface`, so simple mappers can be written as lambdas as shown
above. For more complex implementations, create a class:

```kotlin
class MyMapper : Mapper {
    override suspend fun processMessage(keys: List<String>, datum: Datum): List<Message> {
        // transform, enrich, filter, fan-out, etc.
    }
}
```

All processing methods are `suspend` functions -- implementations can freely call other
suspending APIs (HTTP clients, database queries, etc.) and launch child coroutines.

**Key types:**

| Type | Role |
|------|------|
| `Mapper` | The interface to implement (single `processMessage` method) |
| `Datum` | Input message: `value`, `eventTime`, `watermark`, `headers` |
| `Message` | Output message: `value`, `keys`, `tags` |
| `mapServer` | DSL entry point for creating and starting the gRPC server |
| `MapperConfig` | Server configuration (socket path, port, message size) |

**See also:** `io.numaproj.numaflowkt.mapstreamer` for streaming (incremental) output,
`io.numaproj.numaflowkt.batchmapper` for processing multiple messages in a single call,
`io.numaproj.numaflowkt.sourcetransformer` for transforming at the source vertex with event time reassignment.

# Package io.numaproj.numaflowkt.sinker

Kotlin API for implementing Numaflow **sink** UDFs.

A sink is a terminal vertex in a Numaflow pipeline that writes processed data to an
external system (database, data warehouse, message queue, etc.). The platform delivers
messages in ordered batches -- the implementation must return exactly one `Response` per
input `Datum`, correlated by `Datum.id`, before the next batch is delivered.

Implement `Sinker` and wire it into a gRPC server with `sinkServer`:

```kotlin
import io.numaproj.numaflowkt.sinker.Response
import io.numaproj.numaflowkt.sinker.sinkServer
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList

fun main() {
    sinkServer {
        sinker { messages ->
            messages.map { datum ->
                println("Received: ${String(datum.value)}")
                Response.Ok(datum.id)
            }.toList()
        }
    }.run()
}
```

Messages arrive as a `Flow<Datum>`. Process them by streaming through the flow (as above)
or collect first for bulk operations:

```kotlin
sinker { messages ->
    val datums = messages.toList()
    val results = bulkInsert(datums.map { it.value })
    datums.zip(results).map { (datum, ok) ->
        if (ok) Response.Ok(datum.id) else Response.Failure(datum.id, "insert failed")
    }
}
```

## Response routing

Each `Response` variant controls what happens to the message after the sink processes it:

| Variant | Effect |
|---------|--------|
| `Response.Ok` | Message processed successfully |
| `Response.Failure` | Permanent failure -- message dropped (or retried if a retry strategy is configured) |
| `Response.Fallback` | Route to the fallback (dead-letter) sink |
| `Response.Serve` | Store in a serving store for later retrieval |
| `Response.OnSuccess` | Forward a custom `Message` to the on-success sink |

Returning the wrong number of responses or mismatched IDs will stall the pipeline.

**Key types:**

| Type | Role |
|------|------|
| `Sinker` | The interface to implement (`processMessages`) |
| `Datum` | Input message: `id`, `value`, `keys`, `eventTime`, `watermark`, `headers` |
| `Response` | Sealed class with five result variants |
| `Message` | Payload for `Response.OnSuccess` forwarding |
| `sinkServer` | DSL entry point for creating and starting the gRPC server |
| `SinkerConfig` | Server configuration (socket path, port, message size) |

**See also:** `io.numaproj.numaflowkt.mapper` for per-message transformation (non-terminal).

# Package io.numaproj.numaflowkt.batchmapper

Kotlin API for implementing Numaflow **batch map** UDFs.

A batch mapper receives multiple input messages in a single call, enabling bulk
optimizations such as vectorized ML inference, batch database lookups, or grouped
compression. Each input `Datum` has a unique `Datum.id` and the implementation must
return exactly one `BatchResponse` per input, correlated by that ID.

Implement `BatchMapper` and wire it into a gRPC server with `batchMapServer`:

```kotlin
import io.numaproj.numaflowkt.batchmapper.BatchResponse
import io.numaproj.numaflowkt.batchmapper.Message
import io.numaproj.numaflowkt.batchmapper.batchMapServer
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList

fun main() {
    batchMapServer {
        batchMapper { messages ->
            messages.map { datum ->
                BatchResponse(
                    id = datum.id,
                    messages = listOf(
                        Message(
                            value = String(datum.value).uppercase().toByteArray(),
                            keys = datum.keys
                        )
                    )
                )
            }.toList()
        }
    }.run()
}
```

The example above stream-processes each datum individually. For true batch optimization,
collect the flow first and process in bulk:

```kotlin
batchMapper { messages ->
    val datums = messages.toList()
    val enrichments = bulkLookup(datums.map { String(it.value) })
    datums.zip(enrichments).map { (datum, enriched) ->
        BatchResponse(
            id = datum.id,
            messages = listOf(Message(value = enriched.toByteArray(), keys = datum.keys))
        )
    }
}
```

The total batch size can be up to `readBatchSize` (configured on the pipeline vertex).
If strict ordering is needed, set `readBatchSize` to 1.

**Key types:**

| Type | Role |
|------|------|
| `BatchMapper` | The interface to implement (`processMessage`) |
| `Datum` | Input message: `id`, `value`, `keys`, `eventTime`, `watermark`, `headers` |
| `BatchResponse` | Output per input datum: `id` + list of `Message`s |
| `Message` | Output message: `value`, `keys`, `tags` |
| `batchMapServer` | DSL entry point for creating and starting the gRPC server |
| `BatchMapperConfig` | Server configuration (socket path, port, message size) |

**See also:** `io.numaproj.numaflowkt.mapper` for per-message (non-batch) processing,
`io.numaproj.numaflowkt.mapstreamer` for streaming (incremental) output.

# Package io.numaproj.numaflowkt.mapstreamer

Kotlin API for implementing Numaflow **map stream** UDFs.

A map streamer transforms messages like a regular mapper, but produces output incrementally
as a `Flow` instead of collecting all results before returning. Each emitted `Message` is
forwarded to the downstream vertex immediately, reducing latency when a single input fans
out to many outputs.

Implement `MapStreamer` and wire it into a gRPC server with `mapStreamServer`:

```kotlin
import io.numaproj.numaflowkt.mapstreamer.Message
import io.numaproj.numaflowkt.mapstreamer.mapStreamServer
import kotlinx.coroutines.flow.flow

fun main() {
    mapStreamServer {
        mapStreamer { keys, datum ->
            flow {
                String(datum.value).split(" ").forEach { word ->
                    emit(Message(value = word.toByteArray(), keys = keys))
                }
            }
        }
    }.run()
}
```

In this example each word is sent downstream as soon as it is emitted from the flow,
rather than buffering all words and sending them together. This is particularly
beneficial when:

- A single input expands into many outputs (flat-map with high fan-out)
- Output computation is incremental or async (e.g. paginated API calls)
- Downstream consumers benefit from receiving partial results early

The SDK collects the flow and eagerly forwards each `Message` to the Java SDK's output
observer. After the flow completes, the end-of-transmission marker is sent automatically.

`MapStreamer` is a `fun interface`, so simple streamers can be written as lambdas. The
processing method is `suspend`, allowing async operations (HTTP calls, database queries)
and child coroutine launches from within the flow.

**Key types:**

| Type | Role |
|------|------|
| `MapStreamer` | The interface to implement (`processMessage` returns `Flow<Message>`) |
| `Datum` | Input message: `value`, `eventTime`, `watermark`, `headers` |
| `Message` | Output message: `value`, `keys`, `tags` |
| `mapStreamServer` | DSL entry point for creating and starting the gRPC server |
| `MapStreamerConfig` | Server configuration (socket path, port, message size) |

**See also:** `io.numaproj.numaflowkt.mapper` for non-streaming output (collect all results, then send),
`io.numaproj.numaflowkt.batchmapper` for processing multiple input messages in a single call.

# Package io.numaproj.numaflowkt.sourcetransformer

Kotlin API for implementing Numaflow **source transform** UDFs.

A source transformer is an optional sidecar that runs at the **source vertex** (not a
separate UDF vertex). Its primary purpose is to extract or reassign event times from
message payloads, enabling accurate watermark progression. It can also filter, transform,
and tag messages before they enter the pipeline's inter-step buffer.

This is particularly useful when the source's default event time mechanism is insufficient.
For example, a Kafka source may use `LOG_APPEND_TIME` by default, but the actual event
time is embedded in the JSON payload. A source transformer can parse the payload and
assign the correct event time.

Implement `SourceTransformer` and wire it into a gRPC server with `sourceTransformerServer`:

```kotlin
import io.numaproj.numaflowkt.sourcetransformer.Message
import io.numaproj.numaflowkt.sourcetransformer.sourceTransformerServer
import java.time.Instant

fun main() {
    sourceTransformerServer {
        sourceTransformer { keys, datum ->
            val eventTime = datum.eventTime
            val cutoff = Instant.parse("2022-01-01T00:00:00Z")
            val bucket = Instant.parse("2023-01-01T00:00:00Z")

            when {
                eventTime.isBefore(cutoff) ->
                    listOf(Message.drop(eventTime))
                eventTime.isBefore(bucket) ->
                    listOf(Message(
                        value = datum.value,
                        eventTime = cutoff,
                        keys = keys,
                        tags = listOf("within_2022")
                    ))
                else ->
                    listOf(Message(
                        value = datum.value,
                        eventTime = bucket,
                        keys = keys,
                        tags = listOf("after_2022")
                    ))
            }
        }
    }.run()
}
```

The key difference from a regular mapper is that each output `Message` carries an
`eventTime` which the Numaflow platform uses as the message's event time for watermark
calculation. Even dropped messages require an event time (via `Message.drop(eventTime)`)
so that watermark progression is not blocked.

Because the transformer runs at the source vertex, it saves resources by filtering and
transforming data before it enters the inter-step buffer, avoiding a separate UDF vertex
and the associated network hop.

`SourceTransformer` is a `fun interface`, so simple transformers can be written as lambdas.

**Key types:**

| Type | Role |
|------|------|
| `SourceTransformer` | The interface to implement (`processMessage`) |
| `Datum` | Input message: `value`, `eventTime`, `watermark`, `headers` |
| `Message` | Output message: `value`, `eventTime`, `keys`, `tags` |
| `sourceTransformerServer` | DSL entry point for creating and starting the gRPC server |
| `SourceTransformerConfig` | Server configuration (socket path, port, message size) |

**See also:** `io.numaproj.numaflowkt.mapper` for general-purpose transformation (without event time reassignment).

# Package io.numaproj.numaflowkt.sourcer

Kotlin API for implementing Numaflow **user-defined sources**.

A source is the entry point of a Numaflow pipeline, ingesting data from an external
system (message queue, database, API, custom protocol, etc.) into the streaming pipeline.
Unlike the other UDF types which *process* data, a source *produces* data on demand
through a read/ack/nack lifecycle:

1. The platform calls `Sourcer.read` requesting up to N messages within a timeout
2. Messages flow through the pipeline for processing
3. On success, the platform calls `Sourcer.ack` with the processed offsets
4. On critical failure, the platform calls `Sourcer.nack` to request re-delivery

Implement `Sourcer` and wire it into a gRPC server with `sourceServer`:

```kotlin
import io.numaproj.numaflowkt.sourcer.*
import kotlinx.coroutines.flow.flow
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class SimpleSource : Sourcer {
    private val counter = AtomicInteger(0)
    private val unacked = ConcurrentHashMap.newKeySet<String>()

    override suspend fun read(request: ReadRequest) = flow {
        repeat(request.count.toInt()) {
            val i = counter.getAndIncrement()
            val offsetValue = i.toString()
            unacked.add(offsetValue)
            emit(Message(
                value = "message-$i".toByteArray(),
                offset = Offset(value = offsetValue.toByteArray()),
                eventTime = Instant.now(),
                keys = listOf("key-$i")
            ))
        }
    }

    override suspend fun ack(request: AckRequest) {
        request.offsets.forEach { unacked.remove(String(it.value)) }
    }

    override suspend fun nack(request: NackRequest) {
        // arrange for re-delivery of these offsets
    }

    override suspend fun getPending(): Long = unacked.size.toLong()

    override suspend fun getPartitions(): List<Int> = Sourcer.defaultPartitions()
}

fun main() {
    sourceServer {
        sourcer(SimpleSource())
    }.run()
}
```

The `Sourcer.read` method returns a `Flow<Message>`. The SDK collects the flow, sends each
message to the platform, and automatically sends an end-of-transmission marker when the
flow completes.

**Thread safety:** The sourcer methods may be called concurrently from multiple gRPC
worker threads. Implementations must use thread-safe data structures (`AtomicInteger`,
`ConcurrentHashMap`, etc.) for any shared mutable state.

`Sourcer` is a regular interface (not `fun interface`) because it has five methods.
Implement it as a class as shown above.

**Key types:**

| Type | Role |
|------|------|
| `Sourcer` | The interface to implement (5 methods: read, ack, nack, getPending, getPartitions) |
| `ReadRequest` | Parameters for a read call: `count`, `timeout` |
| `AckRequest` | Offsets to acknowledge as successfully processed |
| `NackRequest` | Offsets to negatively acknowledge for re-delivery |
| `Message` | Output message: `value`, `offset`, `eventTime`, `keys`, `headers` |
| `Offset` | Position identifier in the source: `value`, `partitionId` |
| `sourceServer` | DSL entry point for creating and starting the gRPC server |
| `SourcerConfig` | Server configuration (socket path, port, message size) |

**See also:** `io.numaproj.numaflowkt.sourcetransformer` for transforming messages at the source vertex.
