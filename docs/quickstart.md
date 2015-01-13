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

*If you're feeling particularly opposed to copy/paste, clone the [template project]({{ site.github_template_url }})*

In a standard SBT project layout, create `Main.scala`.  Add this to it:

{% highlight scala %}

import colossus._
import service._
import protocols.telnet._

object Main extends App {

  implicit val io = IOSystem()

  Service.become[Telnet]("hello-world" , 10010) {
    case TelnetCommand("exit" :: Nil) => TelnetReply("Bye!").onWrite(OnWriteAction.Disconnect)
    case TelnetCommand("say" :: text :: Nil) => TelnetReply(text)
    case other => TelnetReply(other.toString)
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
Service.become[Telnet]("hello-world" , 10010) {
{% endhighlight %}

This is using the Service functional DSL to start a server on port 10010 using
the telnet protocol.  Every service is built around a protocol using a
**codec**.  Codecs do the job of turning raw incoming bytes into immutable
request objects, and vise versa for turning response objects into raw bytes.
So for the telnet protocol, your service only has to worry about taking a
`TelnetCommand` and turning it into a `TelnetReply`.

`Service.become` is the simplest form of starting a service, and requires a
pattern-matching expression as its body.  This is perfect for simple, stateless
services like this one, but there are other methods available for more
sophisticated situations.

This brings us to the actual logic of the service:

{% highlight scala %}
case TelnetCommand("exit" :: Nil) => TelnetReply("Bye!").onWrite(OnWriteAction.Disconnect)
case TelnetCommand("say" :: text :: Nil) => TelnetReply(text)
case other => TelnetReply(other.toString)
{% endhighlight %}

This is mostly just standard pattern matching against a `TelnetCommand`, producing a
`TelnetReply` for each case.  The one interesting thing to point out here is this one part:

{% highlight scala %}
TelnetReply("Bye!").onWrite(OnWriteAction.Disconnect)
{% endhighlight %}

`onWrite` is not a method of `TelnetReply`, but actually we are using Scala's
type-lifting pattern to lift the `TelnetReply` into a
`Completion[TelnetReply]`, which lets us attach metadata and some other things
to our response object.  This way we can tell Colossus to terminate the
connection after writing this response, without having to block or deal with
callbacks.

## Let's get Hacking

Writing a service in ~5 lines of code is pretty neat, but obviously this
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
      case request @ Get on Root => request.ok("Hello World!")
    }
  }
}
{% endhighlight %}

Now we're using `Service.serve` instead of `Service.become`.  What's the
difference, and what's with the nested functions?  The goal here is we need an
outgoing connection to Redis, which brings us to the first law of Colossus, *Do
not share state between event loops*.  This means, if we wish to connect to
redis, then we will open one connection per event loop.  Every http connection bound
to a particular event loop will share the same redis connection, which is totally fine
since event loops themselves are single-threaded.

`Service.serve` gives us a place to add initialization code per event loop.
This way we can create the redis connection and it'll simply be in scope for
our pattern matching.  So let's create the connection.

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
      case request @ Get on Root => request.ok("Hello World!")
      case request @ Get on Root / "get" / key => request.notImplemented("soooon")
      case request @ Get on Root / "set" / key / value => request.notImplemented("soooon")
      //anything that falls through the partial function gets turned into a 404 error
    }
  }
}
{% endhighlight %}

So now let's fill in our get route.  Using the redis client is easy:

{% highlight scala %}
case request @ Get on Root / "get" / key => redis.send(Commands.Get(ByteString(key))).map{
  case BulkReply(data) => request.ok(data.utf8String)
  case NilReply => request.notFound("(nil)")
}
{% endhighlight %}

This is where things get interesting.  We're sending a `GET` command to redis
for the key, and mapping on the response.  If you've done concurrent
programming in Scala before, this looks a lot like working with a Future, but
that's not the case here, `redis.send` is returning a type `Callback[Reply]`.
A **Callback** in Colossus behaves very similarly to a Future, but a callback
works entirely in-thread, so even though the above code is non-blocking, it is
also entirely single-threaded.  For the most part, Callbacks have the same
interface as Futures, including `flatMap` and `recover` methods.

Callbacks are the secret sauce that allow Colossus to achieve such low latency
for small, stateless requests like these.  Since both our http connection and
our redis connection live in the same event loop, there's no reason to use a
Future that is designed to work across threads.  The small overhead they need
to run inside an `ExecutionContext` is overkill in this situation.

Callbacks are not meant to replace Futures, in fact the only place they are
used in Colossus is this particular situation where you need to get data from
one connection to another without jumping out of the event loop.  But in the
world of microservices this is basically the most common thing that happens, so
using Callbacks has a significant impact on performance.

Implementing the `Set` route is similar:

{% highlight scala %}
case request @ Get on Root / "set" / key / value => redis.send(Commands.Set(ByteString(key), ByteString(value))).map{
  case StatusReply(msg) => request.ok(msg)
}
{% endhighlight %}

And there we have it, a fully functional service with almost no boilerplate.


### Where to go from here

The rest of docs provide more detail about how all this works and how to
leverage more advanced features, particularly the section on [building a
service server](../serviceserver).  Also be sure to check out the
[examples]({{site.github_examples_url}}) sub-project in the Colossus repo.

