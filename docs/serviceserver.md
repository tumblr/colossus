---
layout: page
title:  "Service Server"
categories: docs
---

## Introduction

A service server is a server designed to process discrete requests into
responses.  Most web servers and databases fall into this category, so the
majority of use cases for Colossus fall under this paradigm.

Essentially a service meets the following goals:

* A server processes individual requests into responses using a well-defined application-layer protocol
* A single connection is linear.  Responses are always returned in the order that their respective requests arrive
* Connections can be (but are not necessarily) pipelined. A server must expect a client to send multiple requests without waiting for any responses.

The service abstraction layer is a fully decoupled system built on top of the
core Colossus framework.  It allows you to build high-performing non-blocking
servers and clients that follow the service paradigm.  Essentially the only
important logic you must handle is the actual processing of requests into
responses.  Colossus will take care of all the linearization and parallelism.

The service layer actually has two different ways of defining a service, one
with a heavy functional feel and the other more object-oriented.  Both styles
are equivalent and ultimately result in the same system being built.  It is
simply up to you to decide which you feel more comfortable with.

## Basic Architecture

Since the service layer is built on top of the core layer, you should familiarize yourself with the structure of a [Colossus server]({{ site.baseurl}}/docs/server) first.

To build a service server we need 3 components:

* A Delegator - This is a simple class that lets you define the behavior of how new connections are accepted.  One delegator is created per event loop.
* A Service Handler - This is a sub-class of `ConnectionHandler` and is the class that handles processing requests into responses.  Just like with any other connection handler, new service handler is instantiated for every accepted connection.
* A Codec - This class handles translating raw bytes into request objects and vise versa for responses.

Colossus currently ships with server codecs for Http, Redis, and Telnet.  


## Build a Service the FP Way

*The [quickstart guide](quickstart) has a more introductory approach to building a service.*

The easiest way to get a simple service up an running is to use the functional DSL.  

*Notice : the DSL is not totally feature complete.  Currently for more advanced functionality you'll have to write your own delegator and connection handler, which is described below*


Using the Http protocol as an example, the general structure of a service looks like:


{% highlight scala %}
Service.serve[Http]("service-name", 456){context: ServiceContext[Http] => 
  //everything here happens once per event-loop
  context.handle{ connecton: ConnectionContext[Http] => 
    //everything here happens once per connection
    connection.become {
      //partial function HttpRequest => Response[HttpResponse]
      case req => req.ok("Hello world!")
    }
  }
}
{% endhighlight %}

This form essentially uses a partial function to map requests to responses, and
provides a way to add initialization to both event loops and individual
connections.  For example, let's add some code to see how things are executed:

{% highlight scala %}



Service.serve[Http]("service-name", 456){context: ServiceContext[Http] => 
  println(s"initializing in worker ${context.worker.id}")
  context.handle{ connecton: ConnectionContext[Http] => 
    println(s"new connection in worker ${context.worker.id}")
    connection.become {
      //partial function HttpRequest => Response[HttpResponse]
      case req => req.ok("Hello world!")
    }
  }
}
{% endhighlight %}

Assuming we are running an `IOSystem` with 2 workers, if we start the service
and open 4 connections to it, we should see in stdout:

{% highlight plaintext %}
initializing in worker 1
initializing in worker 2
new connection in worker 1
new connection in worker 2
new connection in worker 1
new connection in worker 2
{% endhighlight %}




### Clients and Callbacks

One of the most important features of Colossus services are their ability to
interact with connections to external services without leaving the event
loop.  This is ideal when your service is simply passing data back and forth
between clients (of your service) and the external system.

Suppose your service needs to work with a memcache server.  We can open a connection to memecache like this:

{% highlight scala %}
Service.serve[Telnet]("telnet-echo", 456){context => 
  val memcache = context.clientFor[Memcache]("remote-host", 11211)
  //...
{% endhighlight %}

Here we are setting up one memcache connection per delegator, so every
connection bound to an event loop with share the same client connection.  Of
course since event loops are single-threaded, there is no need to worry about
thread-safety or locking a client.

Now in our connection handler, we can do this:

{% highlight scala %}
  def invalidReply(reply: MemcacheReply) = TelnetReply("Invalid reply $reply")

  context.handle{connection =>
    connection.become{
      case TelnetCommand("set", key, value) => memcache.send(Set(key, value)).map{
        case Stored => TelnetReply("ok")
        case other => invalidReply(other)
      }
      case TelnetCommand("get", key) => memcache.send(Get(key)).map {
        case Value(data) => TelnetReply(data.utf8String)
        case NoValue => TelnetReply("(no value)")
        case other => invalidReply(other)
      }
    }
  }
{% endhighlight %}

## Building a Service the OO Way

The functional approach is really just a thin DSL for defining a couple classes.
To directly build a service, we basically need to create classes for our
delegator and connection handler, and wire them up into a server.

First, we'll define our handler

{% highlight scala %}
import com.tumblr.colossus._
import protocols.Telnet._

class HelloWorldHandler(config: ServiceConfig, worker: WorkerRef) 
  extends ServiceServer[HttpRequest, HttpResponse](new HttpServerCodec, config, worker) {

  def processRequest(request: HttpRequest): Response[HttpResponse] = {
    req.ok("Hello World!")
  }

  def processFailure(request: HttpRequest, reason: Throwable) = {
    request.error(s"Error: $reason")
  }
}


{% endhighlight %}

`processRequest` is the primary method for handling requests, `processFailure`
is only used when an uncaught exception is thrown during the processing of a
request (if process Failure throws and exception the connection is terminated).
Because `ServiceServer` inherits `ConnectionHandler`, you also have the ability
to override other handling methods to implement custom shutdown or backpressure
logic.

Implementing a `ServiceServer` instead of a `ConnectionHandler` is the only way
that a service server differs from any other Colossus server.  Thus to complete
our service, we must define a delegator:

{% highlight scala %}
import scala.concurrent.duration._
import colossus.core.{Delegator, ServerRef, WorkerRef}
import colossus.service.ServiceConfig
import trundle.MetricAddress

class HelloWorldDelegator(val server: ServerRef, val worker: WorkerRef) 
  extends Delegator(server, worker) {

  val config = ServiceConfig(
    name = "/hello-world",
    requestTimeout = 100.milliseconds,
  )

  def acceptNewConnection = Some(new HelloWorldHandler(config, worker))

}


{% endhighlight %}

Notice that this is a standard delegator and requires nothing from the service
layer aside from the config.  Similarly, starting the server is the same as
starting any other Colossus server.


{% highlight scala %}
import colossus._
import core.Server

implicit val io_system = IOSystem()

val server = Server(
  name = "echo-server",
  port = 4567,
  delegatorFactory = (server, worker) => new EchoDelegator(server, worker)
)
{% endhighlight %}

## Responses and Completions

When producing a response for a service, we need to deal with two orthoganal situations:

* Handling asynchronous responses, both from Futures and Callbacks
* Attaching metadata to responses

How we handle these situations is an active area of research in Colossus, and our current solution involves using two types.

A `Completion[T]` contains a response value of type `T` as well as metadata such as metrics tags to add and write completion events.

A `Response[T]` is an algebraic datatype (ADT) with 3 implementations:

{% highlight scala %}

sealed trait Response[O]
object Response {
  case class SyncResponse[O](result: Completion[O]) extends Response[O]
  case class AsyncResponse[O](result: Future[Completion[O]]) extends Response[O]
  case class CallbackResponse[O](callback: Callback[Completion[O]]) extends Response[O]
}

{% endhighlight %}

There are also implicit functions to lift each contained type into a `Response`.
So in the above example, our `TelnetReply` objects are automatically converted
into `SyncResponse[Completion[TelnetReply]]` objects.  In the vast majority of cases, you will never
need to worry about lifting them yourself.

The `Response` ADT allows you to write handlers like:

{% highlight scala %}

def processRequest(request: TelnetCommand) = request.cmd.toUpperCase match {
  case "PING" => TelnetReply("PONG")
  case "GET_FUTURE" => (someActor ? GetAFuture).mapTo[String].map{TelnetReply(_)}
  case "CACHE_HIT" => memcacheClient.sendCB(Get("cache_key")).map{TelnetReply(_.data.utf8String)}
}

{% endhighlight %}

So in short, the complete set of types a handler partial function can return (with the built-in implicits in scope) are:

* `T`
* `Future[T]`
* `Callback[T]`
* `Completion[T]`
* `Future[Completion[T]]`
* `Callback[Completion[T]]`
* `SyncResponse[Completion[T]]`
* `AsyncResponse[Future[Completion[T]]]`
* `CallbackResponse[Callback[Completion[T]]]`

In most cases you only ever need to worry about returning the first three.

<div class = "hint">

As with any implicit lifting, occassionally the compiler can raise a somewhat confusing error.

{% highlight scala %}
type mismatch;
[error]  found   : scala.concurrent.Future[com.tumblr.colossus.Completion[Product with Serializable]]
[error]  required: com.tumblr.colossus.Response[com.tumblr.colossus.protocols.Telnet.Telnet#Output]
{% endhighlight %}

This occurs if you attempt to mix types within a `map` or `flatMap` method:

{% highlight scala %}
Service.become[Telnet]("telnet-echo", 456){
  case TelnetReply(a) => Future{a}.flatMap{
    //Future[Completion[TelnetReply]]
    case "a" => Future.successful(TelnetReply("a")).withTags("foo" -> "bar") 
    //Future[TelnetReply]
    case "b" => Future.successful(TelnetReply("b")) 
  }
}
{% endhighlight %}

The solution is to either ensure each case is the same type or indicate the type parameter in the `flatMap` call

{% highlight scala %}
  //do this
  case TelnetReply(a) => Future{a}.flatMap[Telnet#Completion]{

  //or this
    case "b" => Future.successful(TelnetReply("b")).complete
{% endhighlight %}

</div>
