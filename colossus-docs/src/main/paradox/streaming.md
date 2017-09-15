# Streams and Streaming Protocols

@@@ note

**Experimental** : This API is under development and subject to breaking changes in future releases.

@@@

Normally when a server receives or a client sends a message, the entire message
must be in memory and ready to encode/decode all at once.  This is not an issue
for relatively small messages, but when messages start to get larger, anywhere
from a few MB to multiple GB, the requirement for having the entire message available all
at once has both memory and performance implications.

Colossus includes a powerful and efficient API for building _streaming protocols_
where the messages being sent/received are streamed out/in in chunks.  

# The Steaming API

At the heart of streaming is a small set of composable types for building
reactive streams of objects.  The objects themselves can either be pieces of
a larger object (such as chunks of an http message), or full messages
themselves.  You can even have streams of streams.

In fact, Colossus itself uses these components in its own infrastructure for
buffering and routing messages for service connections.  Thus it should be no
surprise that they are highly optimized for speed and efficient
batch-processing.

## Pipes, Sources, and Sinks

A `Pipe[I,O]` is a one-way stream of messages.  Input messages of type `I` are
_pushed_ into the pipe and output messages of type `O` are _pulled_ from it.
In the simplest cases, `I` and `O` will be the same type and the pipe will act
as a buffer, but in many situations a pipe can be backed by a complex
transformation workflow such that the input and output types differ.

@@snip [PipeExamples.scala](../scala/PipeExamples.scala) { #basic_pipe }

Often we want to share a pipe between a producer and consumer, such that the
producer can only push messages and the consumer only pull them.  The `Sink[I]`
and `Source[O]` traits are used for this purpose, as `Pipe[I,O]` implements them
both.

Both pushing and pulling are non-blocking operations.  

@@@ warning

Pipes and other types in the streaming API are **not** thread-safe.  They are
designed to efficiently handle streaming network I/O (which is always
single-threaded in Colossus) and are not intended to be used for general
stream-processing purposes.

@@@

### Pipe Transport States

A Pipe/Source/Sink can be in one of three _transport states_:

* Open : Able to push/pull messages
* Closed : The Pipe has been shutdown without error and no further messages can be pushed/pulled.
* Terminated : The pipe has been shutdown due to an error.

The only possible state transitions are open to closed and open to terminated.

In general, the closed state is for pipes that represent a stream of a single
object like a http message.  Closing the pipe indicates the message has been
fully sent/received.  Terminating a pipe indicate an irrecoverable error has
occurred, such as closing a connection mid-stream.

## Back/Forward-Pressure

Aside from acting as buffers, the primary purpose of Pipes is to efficiently
mediate the asynchronous interactions between producers and consumers of a
stream.  Pipes handle two situations: _back-pressure_ is when
consumers are not able to keep up and need to signal to producers to back-off,
and _forward-pressure_ is when consumers are waiting for work from a producer.
Both situations require signaling mechanisms; producers need to know when
consumers are ready to take on more work and consumers need to know when more
work is available.

### Pushing to Sinks

The primary method of `Sink[I]` is `push(input: I): PushResult`.  The returned
`PushResult` indicates if the message was successfully pushed into the pipe or
what should be done if it was not pushed.  When a Sink is _full_, it is
currently unable to accept any more items and will return a
`PushResult.Full(signal)`.  The contained `Signal` provides a way for the
caller to be notified when the sink is able to accept items.  The caller simply
supplies the signal with a callback function via the `notify` method.

@@snip [PipeExamples.scala](../scala/PipeExamples.scala) { #full_push }

See the docs for @extref[Sink](docs:colossus.streaming.Sink) for more
information on how to push to sinks and also some built-in functions to work
with them.

### Pulling from Sources

Similar to sinks, calling `pull()` on a Source returns a `PullResult[T]`.  This may
be a `PullResult.item(item)` containing the item pulled, or it may be
`PullResult.Empty(signal)`.  This signal can be given a callback which will get
called when there is at least one item to pull from the Source.

@@snip [PipeExamples.scala](../scala/PipeExamples.scala) { #empty_pull }

Another highly efficient way to work with sources is through the `pullWhile`
method which allows you to supply a function that is called whenever an item is
ready to pull.  It also gives you control over whether to continue using the
callback function.

See the documentation for @extref[Source](docs:colossus.streaming.Source) for more
ways to handle pulling items.

### Transforming and Composing Pipes

There are many methods available to build complex pipes by transforming and piecing together existing pipes.

@@snip [PipeExamples.scala](../scala/PipeExamples.scala) { #pipe_compose }


# Streaming HTTP

Currently the only streaming protocol built-into Colossus is Streaming HTTP.
While it shares some types with the standard HTTP protocol, it is a separate
protocol with its own message types.

There are actually two different streaming HTTP API's: a low-level API that
gives you direct control over streams of http messages and message chunks, and
a high-level API that is more like a standard HTTP service.

## Streaming Http Service

The high-level API is more-or-less a standard HTTP service, except that the
body of the request/response is a stream of message chunks.

@@snip [StreamingHttpExample.scala]($examples$/StreamingHttpExample.scala) { #streaming_http }

