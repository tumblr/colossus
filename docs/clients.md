---
layout: page
title: Clients
---

Clients are used to open connections to external systems.  Colossus currently
has built-in support for Http, Memcached, and Redis. Similar to servers, you
can add support for any protocol by implementing the necessary traits and typeclasses.

## Client Behavior in a Nutshell

Clients are intended to be used for long-lived, pipelined connections.  While it
is certainly possible to configure clients to only send one request at a time
or to open a new connection per request, since Colossus is primarily intended to
build long-running services, so too are clients designed for persistent
connections to a single host.

Clients are pipelined and will send multiple requests at once (up to the
configured `inFlightConcurrency` limit, buffering once the limit is hit).

### Connection Management

A Client attempts to open a connection as soon as it is created.  Depending on
how a client is configured, it will immediately accept requests to be sent and
buffer them until the connection is established, so for the most part a Client's
connection management happens in the background.

Similar to server connections, calling `disconnect` on a client will cause it to
immediately reject any new requests, but allow currently in-flight requests to
complete (assuming they don't timeout).  Once a client has been manually
disconnected, it cannot be reused.

If a client unexpectedly loses its connection, it will automatically attempt to
reconnect.  If/how a client reconnects can be controlled by its `connectRetry`
configuration.  During the reconnection, the client will still accept and buffer
new requests (though this can be disabled by setting `failFast` to true).

## Local vs Future Clients

When creating a client, it is up to you to choose how the client handles
concurrency.  In other words, you can choose whether a client uses Colossus
Callbacks or standard Scala Futures.

{% highlight scala %}

val request = HttpRequest.get("/foobar").withHeader("bar", "baz")

//creating this client requires an implicit WorkerRef
val callbackClient: HttpClient[Callback] = Http.client("localhost", 8080)
val response: Callback[HttpResponse] = callbackClient.send(request)

//creating this client requires an implicit IOSystem
val futureClient: HttpClient[Future] = Http.futureClient("localhost", 8080)
val response: Future[HttpResponse] = futureClient.send(request)

{% endhighlight %}

Local clients can only be used in code that runs inside a Worker, in particular
inside a Server or Task.  Local clients are single-threaded and not thread-safe,
but are very fast.  On the other hand, Future clients can be created and used
anywhere, and are thread-safe.  In reality, a future client is just an interface
that sends requests as actor messages to a local client running inside a worker.
But this means that future clients are inherently slower and more resource
intensive, since every request must be sent as an actor message and jump threads
at least twice.

Therefore these two rules may help when choosing what to use:

* When opening a client within a service, use local clients.  Usually you'll want to open one client per worker.
* When opening a client from outside a service, such as from inside an actor or some other general use case, use a future client.

## Failure Management
 
### Client Behavior 

Failures can be broken down into two categories: Connection Failures and 
Request Failures.  Regardless of which type, if a failure occurs the response
is sent back as a failure and NOT retried.  If failfast is enabled, then all
queued requests in the ServiceClient are immediately failed and removed. 
 
If a Request Failure occurs (example: service isn't responding) the response is
sent back as a failure and no subsequent action is taken.

If a Connection Failure occurs (example: connection is closed, host 
unavailable) the response is sent back as a failure and the retry policy (more
below) takes effect.


### Retrying Requests

To enable retries, the LoadBalancingClient (LBC) should be used.  The LBC takes
in a generator that builds a ServiceClient from an InetSocketAddress and a list
of InetSocketAddresses.  The LBC defaults to using an unused host for a request
or the max retry number, whichever is lower.  An example: if the retry count is
set to four but there are only two hosts, it'll only attempt a request twice.

Any failure occurring before the client receives the response (example: 
connection closed, host not responding) is treated as an attempt and is 
retried on the next connection.  If the maximum number of tries are exhausted, 
a SendFailedException is returned with the last exception.

### Example


Below is an example of retrying three times to the same host

{% highlight scala %}
val generator = (address: InetSocketAddress) => {
  Redis.client(address.toString, 6379)
}
val clients = List.fill(3)("myredisserver")
val lbc = new LoadBalancingClient[Redis](worker, generator, 3, clients)

Redis.client(lbc).zadd(ByteString("key"), ByteString("value"))
{% endhighlight %}


### HTTP Example

Below is a simple Http Request example.  When using an Http Call,
a implicit workerRef is required when using the Callback interface.
An implicit ClientCodecProvider is required.

{% highlight scala %}
//only for callbacks, have an implicit executionContext for futures
implicit val workerRef = context.context.worker

import Http.defaults.httpClientDefaults
val httpClient = Http.client("google.com", 80) //Http.futureClient for Futures
val asyncResult = httpClient.send(HttpRequest.get("/#q=mysearch"))
asyncResult.flatMap { result => 
  val idToUpdate = result.utf8String.toLong
  //execute business logic from teh fetch here
}
{% endhighlight %}


### Memcached Example

Below is a simple memcached get example.  It can be tested locally
by starting up a memcached server and setting a value for the key of 1.
The equivalent command would be "get 1" from a telnet session on 11211.  
When using the memcached client an implicit workerRef is required when using the Callback
interface.  An implicit ClientCodeProvider is required.

{% highlight scala %}
//only for callbacks, have an implicit executionContext for futures
implicit val workerRef = context.context.worker

import colossus.protocols.memcache.Memcache.defaults.memcacheClientDefaults

//switch Memcache.futureClient for Futures
val client = Memcache.client("localhost", 11211) 
val asyncResult = client.get(ByteString("1"))
asyncResult.flatMap { result => 
  val idToUpdate = result.utf8String.toLong
  //execute business logic from teh fetch here
}
{% endhighlight %}


### Redis Example

Below is a simple redis get example.  It can be tested locally
by starting up a redis server and setting a value for the key of 1.
When using the Redis client an implicit workerRef is required when using the Callback
interface.  An implicit ClientCodeProvider is required.


{% highlight scala %}
//only for callbacks, have an implicit executionContext for futures
implicit val workerRef = context.context.worker

import colossus.protocols.redis.Redis.defaults.redisClientDefaults
val myClient = Redis.client("localhost", 6379) //Redis.futureClient for Futures
val asyncResult = myClient.get(ByteString("1"))
asyncResult.flatMap { result => 
  val idToUpdate = result.utf8String.toLong
  //execute business logic from teh fetch here
}
{% endhighlight %}


### Retry Policy

In the event of a Connection Failure, the service client uses a 
[RetryPolicy](https://tumblr.github.io/colossus/api/index.html#colossus.core.RetryPolicy)
for re-establishing connections.  Each service client takes in a RetryPolicy on
creation.  The client defaults to an exponential backoff starting at 50 
milliseconds and with a maximum of 5 seconds.  The policy type can be either 
[NoRetry](https://tumblr.github.io/colossus/api/index.html#colossus.core.NoRetry) 
or [BackoffPolicy](https://tumblr.github.io/colossus/api/index.html#colossus.core.BackoffPolicy).

A Backoff retry policy contains a multiplier which can be:
 
 * Constant
 * Linear
 * Exponential

## Using clients generically

Notice that the concurrency type (Callback/Future) is encoded in the type of the
client.  The clients abstract over these two types with the `Async` typeclass.
If you wish to write generic code that works with a client regardless of which
concurrency type is used, simply pull in an `Async` implicitly and you're good
to go:

{% highlight scala %}

def doTheThing[A](client: HttpClient[A])(implicit async: Async[A]) : A[Int] = {
  client.send(HttpRequest.get("/foo")).map{response => 
    response.body.bytes.utf8String.toInt
  }
}

//now you can do
doTheThing(HttpClient.client("localhost",8080))
doTheThing(HttpClient.futureClient("localhost", 8080))

{% endhighlight %}

To do things even more generically, all clients extends the `Sender[P,A]` trait.

