# Clients

Clients are used to open connections to external systems.  Colossus currently
has built-in support for Http, Memcached, and Redis. Similar to servers, you
can add support for any protocol by implementing the necessary traits and typeclasses.

## Client Behavior in a Nutshell

Clients are intended to be used for long-lived, pipelined connections.  While it
is certainly possible to configure clients to only send one request at a time
or to open a new connection per request, since Colossus is primarily intended to
build long-running services, so too are clients designed for persistent
connections to a single host.

A client can open many connections to the same or different external systems depending on how it is setup. If
it opens many connections then it will load balance between the connections, sending different requests on different
connections.

A Client will attempt to open its connections as soon as it is created.  Depending on
how a client is configured, it will immediately accept requests to be sent and
buffer them until the connection is established, so for the most part a Client's
connection management happens in the background.

Similar to server connections, calling `disconnect` on a client will cause it to
immediately reject any new requests, but allow currently in-flight requests to
complete (assuming they don't timeout).  Once a client has been manually
disconnected, it cannot be reused.

If a client unexpectedly loses one of its connection, it will automatically attempt to
reconnect.  If/how a connection reconnects can be controlled by its `connectRetry`
configuration.  During the reconnection, the client will still accept and buffer
new requests (though this can be disabled by setting `failFast` to true).

Clients are pipelined and will send multiple requests at once (up to the configured `inFlightConcurrency` limit, 
buffering once the limit is hit).

If a client is setup with multiple connections, then a request failure will automatically be retried on another 
connection. The request will be retried on each of the connections until success or each connection has been tried. The
retry policy can be configured by altering `request-retry-policy`, such that retries can be turned off and backoff can
be applied between retry attempts. Retry attempts can be set higher than available connections, in which case the same
connection could be reused.

## Local vs Future Clients

When creating a client, it is up to you to choose how the client handles
concurrency.  In other words, you can choose whether a client uses Colossus
Callbacks or standard Scala Futures, although the decision will usually depend on where you are trying
to use the client.

Local clients can only be used in code that runs inside a Worker, in particular
inside a Server or Task.  Local clients are single-threaded and not thread-safe,
but are very fast.

@@snip [CallbackClient.scala](../scala/CallbackClient.scala) { #callback_client }

On the other hand, Future clients can be created and used
anywhere, and are thread-safe.  In reality, a future client is just an interface
that sends requests as actor messages to a local client running inside a worker.
But this means that future clients are inherently slower and more resource
intensive, since every request must be sent as an actor message and jump threads
at least twice.

@@snip [FutureClient.scala](../scala/FutureClient.scala) { #future_client }

Therefore these two rules may help when choosing what to use:

* When opening a client within a service, use local clients.
* When opening a client from outside a service, such as from inside an actor or some other general use case, use a future client.

## Retry Policy

In the event of a connection failure or request failure, the client uses a @extref[RetryPolicy](docs:colossus.core.RetryPolicy)
to determine what to do next.

The connection retry policy defaults to an exponential backoff starting at 50 milliseconds with a maximum of 5 seconds,
and will retry forever. The request retry policy defaults to having no backoff between retries and will retry up to
the number of connections.

The policy type can be either @extref[NoRetry](docs:colossus.core.NoRetry) or 
@extref[BackoffPolicy](docs:colossus.core.BackoffPolicy). A backoff retry policy contains a multiplier which can be:

 * Constant
 * Linear
 * Exponential

Below is an example of setting up a client to retry three times:

@@snip [RedisRetryClient.scala](../scala/RedisRetryClient.scala) { #example }


## Examples

### HTTP Example

Below is a simple Http Request example.  When using an Http Call,
a implicit workerRef is required when using the Callback interface.
An implicit ClientCodecProvider is required.

@@snip [HttpClientExample.scala](../scala/HttpClientExample.scala) { #example }


### Memcached Example

Below is a simple memcached get example.  It can be tested locally
by starting up a memcached server and setting a value for the key of 1.
The equivalent command would be "get 1" from a telnet session on 11211. 

When using the memcached client an implicit workerRef is required when using the Callback
interface.  An implicit ClientCodeProvider is required.

@@snip [MemcacheClient.scala](../scala/MemcacheClient.scala) { #example }


### Redis Example

Below is a simple redis get example.  It can be tested locally
by starting up a redis server and setting a value for the key of 1.
When using the Redis client an implicit workerRef is required when using the Callback
interface.  An implicit ClientCodeProvider is required.


@@snip [RedisClientExample.scala](../scala/RedisClientExample.scala) { #example }



## Generic clients

Notice that the concurrency type (Callback/Future) is encoded in the type of the
client.  The clients abstract over these two types with the `Async` typeclass.
If you wish to write generic code that works with a client regardless of which
concurrency type is used, simply pull in an `Async` implicitly and you're good
to go:

@@snip [GenericClient.scala](../scala/GenericClient.scala) { #example }

To do things even more generically, all clients extends the `Sender[P,A]` trait.
