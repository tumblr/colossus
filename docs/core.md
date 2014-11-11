---
layout: page
title:  "Core Layer"
categories: docs
---

The Colossus Core layer is an actor-driven implementation of the multi-reactor model on NIO.

## Event loops are Actors

The defining characteristic of the core layer is that _event loops are actors_.
In most other systems, a typical event loop would look like (in pseudocode):

{% highlight scala %}
while (true) {
  event_handlers = selector.select()
  for each handler in event_handlers {
    handler.handleEvent
  }
}
{% endhighlight %}

In Colossus, the physical iteration of the event loop is handled by the
actor sending a message to itself.

{% highlight scala %}
receive message {
  case `Select` => {
    event_handlers = selector.select()
    ...
    self ! `Select`
  }
  ...
}
{% endhighlight %}

This means that Colossus event loops can receive messages and process them in
between each event loop iteration.  Furthermore, the event handlers attached to
connections are able to tap into the actor's mailbox, such that an event
handler has the ability to send and receive messages both to itself and other
actors.

## Actor-driven, not Actor-based

In an actor-based model (such as Akka's own IO layer), everything is an actor
including user event handlers.  The event loop and the event handlers interact
using Actor message passing.  Colossus does not work this way.  While the event
loops are actors, event handlers are simply POJO's (POSO?) and all
interactions are through regular function calls.

One of the reasons for doing this is that even though message passing is a
fairly lightweight operation, it is not nearly as lightweight as just a
function call, and in a massively concurrent system handling millions of events
per second, the difference in overhead is non-trivial.  This is not a
theoretical conjecture, but is an empirical observation that initially led to
the development of Colossus.

Another reason for fully embedded event handlers is the ability to run code
directly inside the event loop.  Like other reactive frameworks, this can allow
you to simplify low-level logic since all the code inside an event handler is
single-threaded (the service layer extensively takes advantage of this
feature).

## The IO System

The heart of any Colossus project is the IO System.  This is essentially a
collection of event loops, each running as an Akka actor.  These actors, called
Workers, handle all I/O for the system.  Once an IO System is created, servers
and clients can be attached to the system.  Servers, which listen on a port and
accept incoming connections, will multiplex new connections across workers.
Clients attach to exactly worker, however as we will see you will generally
create one client per worker.  In both cases, once a connection has been
established and attached to a worker it will remain attached to that worker for
the duration of its lifetime.

Creating an IO System is easy, all you need is an actor system:

{% highlight scala %}

implicit val actor_system = ActorSystem("system")
val io_system = IOSystem("io-system", numWorkers = 4)

{% endhighlight %}

Here we're creating an IO System with 4 worker event loops.

While  many servers and clients can be attached to a single system, IO Systems
are lightweight, and multiple IO Systems can exist simultaneously and can be
attached to the same actor system.  An IO System with n workers will create n +
1 actors and n threads.  So if you have a single application that needs to
start up 2 independantly running servers, starting each server in its own IO
System is not a bad idea.

### Worker Isolation and Worker-local state

One of the most important aspects of workers is that being actors, they do not
(and should not!) inherently share any mutable state.  This is why client
connections must be setup for every worker, since two workers will not use the
same connection.  Of course, you can get around this by creating an
asynchronous wrapper around a client using an actor, and handing off the
ActorRef to each worker, but in most cases whatever you're connecting to will
be ok with having multiple connections open.

Inside a worker, mutable state can and should be shared when appropriate.  For
example, all server connections on a single worker share the same interface for
recording metrics.  Server connections on a worker can also share client
connections.

