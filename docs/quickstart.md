---
layout: page
title:  "Quickstart"
categories: docs
---

Hey there, thanks for taking a look!  This quickstart is designed to give a
brief introduction to some of the central features of Colossus and show what
it's like to work with the framework.

This guide covers the primary focus of Colossus, building services.  A
**service** is a server that reads requests from clients and processes them
into responses to send back.  Clients can send multiple requests at the same
time, many (thousands of) clients can be connected to a server at the same
time, and request processing is expected to happen in parallel.  Colossus does
all that and more.

This quickstart assumes you are familiar with developing Scala applications
using SBT.  Furthermore you should be generally familiar with Akka and
concurrent programming concepts.

## SBT
Add the following to your Build.scala or build.sbt:

{% highlight scala %}

libraryDependencies += "com.tumblr" %% "colossus" % "{{ site.latest_version }}"

{% endhighlight %}

Colossus is compiled for Scala 2.10 and 2.11 and built against Akka 2.3.

## Build a Hello World Service

In a standard SBT project layout, create `Main.scala`.  Add this to it:

{% highlight scala %}

import colossus._
import service._
import protocols.telnet._

object Main extends App {

  implicit val io = IOSystem()

  Service.serve[Telnet]("hello-world" , 10010) { context => 
    context.handle{ connection => 
      connection.become {
        case TelnetCommand("say" :: text :: Nil) => {
          Callback.successful(TelnetReply(text))
        }
        case TelnetCommand("exit" :: Nil) => {
          connection.gracefulDisconnect()
          Callback.successful(TelnetReply("Bye!"))
        }
        case other => {
          Callback.failed(new IllegalArgumentException(s"Invalid command $other"))
        }
      }
    }
  }

}

{% endhighlight %}

Now when you run your application, you should be able to connect to the server with any standard telnet client.

{% highlight plaintext %}
> telnet localhost 10010
Trying ::1...
Connected to localhost.
Escape character is '^]'.
> hello
TelnetCommand(List(hello))
> say "I am a teapot"
I am a teapot
> exit
Bye!
Connection closed by foreign host.

{% endhighlight %}

### A Closer Look

Let's look at this service line-by-line.

{% highlight scala %}
implicit val io = IOSystem()
{% endhighlight %}

The first thing you have to do when starting any Colossus service is to create
an `IOSystem`.  The `IOSystem` is a collection of event loops with a thin
management layer on top.  Both Servers and clients can be attached to an
`IOSystem`, which by default will start one event-loop per physical CPU core.
Internally, an `IOSystem` is just a bunch of Akka actors, and in this context
the `IOSystem` will create its own Akka `ActorSystem`, but you can also attach
it to an existing `ActorSystem`.

In the case of a server like our service, all of the event loops in the
`IOSystem` will be used, with incoming connections being round-robin'd across
them.  This means that overall our service is multi-threaded, but all our code
interacting with a particular connection is single-threaded (at least so far).

We defined the `IOSystem` as `implicit` because it is a dependency for the next
line, which is where we actually spin up our service:

{% highlight scala %}
Service.serve[Telnet]("hello-world" , 10010) { context => 
{% endhighlight %}

This starts a server on port 10010 using the telnet protocol.  Every service is
built around a protocol using a **codec**.  Codecs do the job of turning raw
incoming bytes into immutable request objects, and vise versa for turning
response objects into raw bytes.  So for the telnet protocol, your service only
has to worry about taking a `TelnetCommand` and turning it into a
`TelnetReply`.

The `context` is a reference to the current event loop.  Here we don't need it,
but it provides the interface for interacting directly with the event loop.

This brings us to the actual logic of the service:

{% highlight scala %}
case TelnetCommand("say" :: text :: Nil) => {
  Callback.successful(TelnetReply(text))
}
case TelnetCommand("exit" :: Nil) => {
  connection.gracefulDisconnect()
  Callback.successful(TelnetReply("Bye!"))
}
case other => {
  Callback.failed(new IllegalArgumentException(s"Invalid command $other"))
}
{% endhighlight %}

This is mostly just standard pattern matching against a `TelnetCommand`, producing a
`Callback[TelnetReply]` for each case.  A `Callback` is Colossus' internal
mechanism for asychronous actions within an event loop.  It is a monad and is
used very similarly to a `Future`, but only for within an event loop.  This
example doesn't really illustrate the need for Callbacks, so we'll cover these
in more detail in the next section.

## Let's get Hacking

Writing a service in ~10 lines of code is pretty neat, but obviously this
example is artificially simple and doesn't really illustrate the real power of
Colossus.  Let's try doing something a little more interesting, such as writing
a http interface to a redis server.

### Writing the Http service

So we want to build a service that starts an http server and also connects to a
redis database.  First let's create a skeleton service using the http protocol:

{% highlight scala %}
import protocols.http._
import HttpMethod._
import UrlParsing._

Service.serve[Http]("http-service", 9000){ context =>
  context.handle{ connection =>
    connection.become{
      case request @ Get on Root => Callback.successful(request.ok("Hello World!"))
    }
  }
}
{% endhighlight %}

Now it's time to setup our connection to Redis.  In this case, we're going to
create one client connection per event loop, allowing all http connections in
the same event loop to share a single redis connection.  This gives us service
that conceptually looks like:

![redis]({{site.baseurl}}/img/redis.png)

This may not always be the best layout for every situation (especially for
systems not as consistently low-latency as redis), but here it allows us to do
two things.  First, we can keep persistent connections open to redis, which is
ideal since the overhead of establishing a TCP connection is likely to take
much longer than any `GET` or `SET` command to redis.  Second, it allows us to
perform all I/O operations within the event loop, and since event-loops are
single threads we can totally avoid using any parallelism mechanism such as
locks or Futures, cutting down on latency.

To open one redis connection per event loop, we do:

{% highlight scala %}
import protocols.redis._
import akka.util.ByteString

Service.serve[Http]("http-service", 9000){ context =>
  val redis = context.clientFor[Redis]("localhost", 6379)
  //...
{% endhighlight %}

So on a computer with 4 cores, this code will end up opening 4 connections to
redis.  Here we're using the default config, but clients have a ton of
configuration options such as setting various buffer sizes and failure logic.
Also like servers, clients have metrics such as request rate and latency
built-in.

So all together now, along with some dummy routes for get/set on keys:

{% highlight scala %}

implicit val io = IOSystem()

Service.serve[Http]("http-service", 9000){ context =>
  val redis = context.clientFor[Redis]("localhost", 6379)
  context.handle{ connection =>
    connection.become {
      case request @ Get on Root => {
        Callback.successful(request.ok("Hello World!"))
      }
      case request @ Get on Root / "get" / key => {
        Callback.failed(new NotImplementedException("soon"))
      }
      case request @ Get on Root / "set" / key / value => {
        Callback.failed(new NotImplementedException("soon"))
      }
    }
  }
}
{% endhighlight %}

So now let's fill in our get route.  Using the redis client is easy:

{% highlight scala %}
case request @ Get on Root / "get" / key => redis.send(Commands.Get(ByteString(key))).map{
  case BulkReply(data) => request.ok(data.utf8String)
  case NilReply        => request.notFound("(nil)")
  case _               => request.error("Invalid response from redis")
}
{% endhighlight %}

This shows how Callbacks are used to interact between server connections (http)
and client connections (redis).  We're sending a `GET` command to redis for the
key, which returns a `Callback[Reply]`, and then mapping on it to turn it into
a `Callback[HttpResponse]`.  This should look very familar to anyone who has
worked with Futures, and indeed Callbacks have analogs of all the typical
methods on `Future`, including `flatMap` and `recover`.  The big difference is
that the execution of a Callback never leaves the event-loop.

Implementing the `Set` route is similar:

{% highlight scala %}
case request @ Get on Root / "set" / key / value => {
  redis.send(Commands.Set(ByteString(key), ByteString(value))).map{
    case StatusReply(msg) => request.ok(msg)
    case _                => request.error("Invalid response from redis")
  }
}
{% endhighlight %}

And there we have it, a fully functional service with almost no boilerplate.


### Where to go from here

The rest of docs provide more detail about how all this works and how to
leverage more advanced features, particularly the section on [building a
service server](../serviceserver).  Also be sure to check out the
[examples]({{site.github_examples_url}}) sub-project in the Colossus repo.

