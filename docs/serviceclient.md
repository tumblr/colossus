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

