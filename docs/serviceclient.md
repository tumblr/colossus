---
layout: page
title:  "Service Clients"
categories: docs
---

## Using Service Clients

There are essentially two type of service clients, _internal_ clients and _asynchronous_ clients.  In reality, async clients are just a wrapper around internal clients, but the use cases for each one are different.

* An internal client should only be used inside a worker.  This means they should only be created inside delegators, connection handlers, or tasks.
* An async client is basically just an actor that is hooked up to an internal client.  Async clients are useful if code outside an IO System needs access to a client.

### Internal Client

Internal clients are directly attached to a worker and are mostly designed for
low-latency non-blocking communication with an external system.

Here's a basic example:

{% highlight scala %}
  import protocols.redis._
  import UnifiedProtocol._

  Task{context => 
    import context._

    //setup the client
    val config = ClientConfig(
      name = "/redis-client",
      requestTimeout = 50.milliseconds
    )
    val redis = new ServiceClient(new RedisClientCodec, config, worker)
    redis.connect()

    //this will be called when we're finished
    def terminate(message: String) {
      println(message)
      redis.disconnect()
      unbind()
    }

    //send a command (non-blocking)
    redis.send(Command("SET", "a_key", "a_value")) {
      case Success(StatusReply(_)) => redis.send(Command("GET", "a_key")) {
        case Success(BulkReply(bytes)) => terminate(s"Got ${bytes.utf8String})
        case other => terminate(s"Invalid GET Reply: $other")
      }
      case other => terminate(s"Invalid SET Reply: $other")
    }
  }
{% endhighlight %}

It is important to realize that not all of this code is executed at once, and
also that this code is completely single-threaded and non-blocking.  When the
client's `send` method is called, you are passing to it both a command as well
as a callback for handling the reply.  The client internally buffers this and
then immediately returns.

### Async Client

{% highlight scala %}

implicit val io_system = //....

//setup the client
val config = ClientConfig(
  name = "/redis-client",
  requestTimeout = 50.milliseconds
)

val client: ActorRef = AsyncServiceClient(config, new RedisClientCodec)

def terminate(message: String) {
  println(message)
  client ! PoisonPill
}

(client ? Command("SET", "a_key" , "a_value")).onComplete{
  case Success(StatusReply(_)) => (client ? Command("GET", "a_key")).onComplete{
    case Success(BulkReply(data)) => terminate(s"Got ${data.utf8String}")
    case other => terminate(s"Invalid GET Reply: $other")
  }
  case other => terminate(s"Invalid SET Reply: $other")
}

{% endhighlight %}

Here we can see we're interacting with the client as we would with any other actor.

Because of the overhead of using Futures and Actors, an `AsyncServiceClient` will be considerably slower than an internal client

## Monadic Callbacks

Colossus comes with an implementation of callbacks as monads.  In the above
example of using an internal client, we're using the standard approach to
callback.  the `send` method has the signature:

{% highlight scala %}

class ServiceClient[Request,Response]... {
  
  def send(request: I)(handler: Try[Response] => Unit)
}

{% endhighlight %}

This approach, where the function takes a request and a closure as a response
handler, is very common for event-based systems.  While this approach certainly
works, it makes it very difficult to build callbacks in a functional way, since
the closure must be completely built at the time of invocation.

In particular, how to we handle a situation where we are building a service
server, and as part of a request we wish to call an internal client?  The
server's `processRequest` requires us to return something, but the client's
`send` method does not return anything.  Of course, normally this is where we
may use a `Future` (and internal clients do have a `sendAsync` method that does
return a `Future\[Response]`), but in low-latency situations, Futures can add a
significant amount of unnecessary overhead.

Colossus has another approach by using a `Callback[Response]` type, which at
first glance behaves a lot like a Future.  Callbacks, though, are purely designed
for the use-case of getting data between server and client connections with as
little overhead as possible, and thus are more performant in this situation.

_NOTE - It's important to realize that callbacks are an optimization which can
give a significant performance boost, but like any specialized optimization
they come with drawbacks.  When in doubt, use Futures_

Clients have another method called `sendCB`

{% highlight scala %}
class ServiceClient[Request,Response]... {

  def sendCB(request: I): Callback[Try[Response]]

}

{% endhighlight %}

This approach allows you to make a request to a service client and return the
callback in your service server.  Callbacks have a the usual monadic transformaers:

{% highlight scala %}

trait Callback[O] {
  def map[U](f: O => U): Callback[U]
  def flatMap[U](f: O => Callback[U]): Callback[U]
}

{% endhighlight %}

as well as some useful utility functions

{% highlight scala %}

object Callback {
  def sequence[O](callbacks: Seq[Callback[O]]): Callback[Seq[O]]
  def done[O](value: O): Callback[O]
}

{% endhighlight %}


