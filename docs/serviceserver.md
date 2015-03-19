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

## Writing a Service

### A Basic service

The [quickstart guide](quickstart) has a more introductory approach to building a service.

The general form of starting a service looks like :


{% highlight scala %}

Service.serve[Protocol]("name", 9000) { context =>
  context.handle { connection =>
    connection.become {
      //partial function
    }
  }
}

{% endhighlight %}

Let's break down what's happening here:

The `Protocol` type parameter is a `CodecDSL`, which is a trait that describes the input
and output types of a protocol.  For example, to start a server using the http
protocol, we require the `Http` trait.  

{% highlight scala %}

import colossus.protocols.http._

Service.serve[Http]("http-service", 9000) { context => 
//...

{% endhighlight %}

More about Codecs and Protocols can be found [here]().

We now have a few nested closures.  Each closure takes us closer to processing
an individual request

{% highlight scala %}

Service.serve[Protocol]("name", 9000) { context =>
  //worker context - everything here is executed once per event loop
  context.handle { connection =>
    //connection context - everything here is executed once per connections
    connection.become {
      //request context - everything here is executed once per request
    }
  }
}

{% endhighlight %}

The **Worker Context** is the place to add initialization per event loop.  In
most cases, this is where client connections are established, metrics are
setup, and other long-term initialization is performed.

The **Connection Context** is the place to add initialization per new connection.

The **Request Context** is a partial function that processes requests into
responses.  Any request that fails to be matched is automatically converted
into an error response defined by the protocol.



### Using clients

Service Clients provide in-thread, asynchronous client connections to external services.  Similar to servers, a client requires a protocol:

{% highlight scala %}

Service.serve[Protocol]("name", 9000) { context =>
  val client = context.clientFor[ClientProtocol]("host", port)
  //...

}

{% endhighlight %}

In general, service clients are intended to be used for long-running persistent
connections, such as to a cache, database, or other service.  So while it is
perfectly fine to open a client connection per server connection (or even per
request), the best place to start is per event loop.  

Clients have numerous configuration options available concerning timeouts and
failure handling.  By default if a client loses its connection, either by
timing out, remote closing, or an error, it will automatically re-establish a
connection and (depending on configuration) buffer requests during the
reconnection period.

### Interacting with Actors and Futures

Because the worker, connection, and request contexts all are single-threaded and execute in
the event loop, performing certain tasks in-thread are either impractical or
impossible.  For example, a CPU-intensive operation or blocking API call while
processing one request would end up pausing the event loop, causing latency
spikes for any other requests being processed in the loop.

Colossus was built with these use cases in mind and makes it easy to interact
with Futures and Akka Actors.

Suppose we have a function `doTask()` that returns a `Future[T]`.  To execute this method in the processing of a request, we can do:

{% highlight scala %}

Service.serve[Protocol]("name", 9000) { context =>
  import context.callbackExecutor
  context.handle { connection =>
    connection.become {
      case request => Callback.fromFuture(doTask()).map{result => ProtocolResponse(result)}
    }
  }
}

{% endhighlight %}

`Callback.fromFuture` converts a `Future[T]` into a `Callback[T]`.  This
requires an implicit `CallbackExecutor` which is basically a reference to the
event loop that should resume the processing of the Callback after the Future
has completed execution.

Thus, `fromFuture` gives us a way to "jump" out of the event-loop to do some
work in another thread, and then jump back into the event-loop after it
complete.  So any `map`, `flatMap`, or `recover` that occurs after `fromFuture`
is still executed in the event loop and is thus thread-safe.

It is very important not to accidentally call thread-local code from within a Future.  Take a look at the following service:

{% highlight scala %}

Service.serve[Protocol]("name", 9000) { context =>
  var num = 0
  import context.callbackExecutor
  context.handle { connection =>
    connection.become {
      case RequestTypeA => Callback.fromFuture(doTask()).map{result =>
        num += 1
        ProtocolResponse(result)
      }
      case RequestTypeB => Callback.fromFuture(doTask().map{result => 
        num += 1
        result
      }).map{result => ProtocolResponse(result)}
    }
  }
}

{% endhighlight %}

The case for `RequestTypeA` is thread-safe, since the increment `num` happens
after the Future has been converted to a Callback, and all code executed inside
a Callback is thread-safe.  However, the case for `RequestTypeB` is **not**
thread-safe, since the `map` that increments `num` is a map on the Future, not
on the Callback.

In general, the same precautions should be taken as when working with Futures
inside Actors: never expose local state.



