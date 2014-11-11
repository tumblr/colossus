---
layout: page
title:  "Servers"
categories: docs
---

## Introduction

A server is one of the primary components of Colossus.  A server listens for
TCP connections on a single port and distributes them across all the workers in
an IOSystem.

A Server consists of 3 parts, two of which are user-defined:

1.  Server actor.  This actor runs its own event loop and handles the process
of opening a socket and listening for incoming connections.  Server actors are all identical.

2.  Connection Handler.  For every newly accepted connection, a connection handler is atteched that handles all interactions with the connection.  Connection Handlers are what contain the low-level business logic for the server.

3.  Delegator.  This is a simple class that is in charge of creating new ConnectionHandler's for each new connection.  Delegators live inside workers and run as part of the worker's event loop


![img]({{ site.baseurl }}/img/iosystem.png)

## Creating a Server

In most cases, you will generally not be creating servers from scratch, but rather using the [services]({{ site.baseurl}}/docs/serviceserver) abstraction layer.

### Starting a Server

In order to spin up a new server, it must be attached to an IO System.  This can be done by using the `Server` object:

{% highlight scala %}

implicit val io_system = //...

val serverConfig = ServerConfig(
  name = "my-first-server",
  timeoutConfig = TimeoutConfig.Default.copy(maxConnections = 1000),
  port = 2345,
  delegatorFactory = (server, worker) => new MyFirstDelegator(server, worker)
)
val server = Server(serverConfig)

{% endhighlight %}


To attach a server to an IO System, you must give it a `DelegatorFactory`,
which is literally just a lambda that returns new instances of your delegator.
When a server is first attached to an IO System, it registers itself with the
system, and part of this process involves sending that lambda to every worker
in the system.  The workers will then use it to create a new instance of your
delegator.  From that point on, any time your server accepts a new connection,
the connection is forwarded to a worker, which then uses its delegator to
create a `ConnectionHandler` for the connection.

### Delegators

Delegators serve the purpose of providing `ConnectionHandler`s for incoming
connections.  These are long-lived objects that are created once by every
worker in an IO System.

{% highlight scala %}

class Delegator(server: ServerRef, worker: WorkerRef) {
  def acceptNewConnection: Option[ConnectionHandler]
}

{% endhighlight %}

an example of a basic delegator simply just creates a new `ConnectionHandler`

{% highlight scala %}

class MyFirstDelegator(server: ServerRef, worker: WorkerRef) 
  extends Delegator(server, worker) {

  def acceptNewConnection = Some(new MyFirstConnectionHandler)
}

{% endhighlight %}

Generally, for more complex system, delegators are where you can initialize
clients to external systems, setup metrics, or perform other actions that are
common to all handlers on a worker.

**IMPORTANT** : Remember that one delegator is created _for every worker_ in an
IOSystem, so any shared resource you create inside a delegator is only shared
by the connections attached to that worker.  If you need a globally shared
resource like an `ActorRef`, it should be passed to your delegator as a
constructor parameter, eg:

{% highlight scala %}

val someActor = system.actorOf(//...

val delegator_factory = 
  (server, worker) => new SoemDelegator(someActor, server, worker)

{% endhighlight %}

### Connection Handler

The `ConnectionHandler` is where most of your server's user-code will live.  A
`ConnectionHandler` is created for every new connection and is attached to the
connection for as long as it's open.  The `ConnectionHandler` trait has a bunch
of methods that are called when various events occur on its attached
connection:

{% highlight scala %}

trait ConnectionHandler extends WorkerItem {
  def receivedData(data: DataBuffer)
  def connectionClosed() 
  def connectionLost() 
  def readyForData()
  def connected(endpoint: WriteEndpoint)
}

{% endhighlight %}

_NOTE: When building services, you will not be implementing this trait, but
instead the considerably simpler `ServiceServer` sub-trait.  See the section on
building [services]({{ site.baseurl}}/docs/serviceserver) for more information._


## A Few Words on Thread Safety

It's important to understand where code is executed in order to understand what must be done to ensure thread-safety.

Since a worker is just an actor, many of the conventions around actors also
apply to delegators and connection handlers.  In general, all code inside
delegators and connection handlers is run inside the worker and is therefore
thread-safe, but _mutable state must not be shared across workers_.  This means
that connections can share safely share state with their creating delegator,
and connections created by the same delegator can share state, but two
delegators should _not_ share mutable state, nor should all connections for a
single server.

**OK**

{% highlight scala %}

class FooDelegator(server: ServerRef, worker: WorkerRef) 
  extends Delegator(server, worker) {

  val stuff = collection.mutable.Map[String, String]()

  def acceptNewConnection = Some(new FooHandler(stuff))
}

{% endhighlight %}

This is ok because the mutable `Map` is local to the delegator, and it is ok
for all connections created by the delegator to share it.  **But**, you should
be aware that in an IO System with 10 workers, there will be 10 separate
`Map`s, one created by each instantiated delegator.

**NOT OK**

{% highlight scala %}

class FooDelegator(stuff: mutable.Map[String, String], server: ServerRef, worker: WorkerRef) 
  extends Delegator(server, worker) {

  def acceptNewConnection = Some(new FooHandler(stuff))
}
{% endhighlight %}

This code is not thread-safe because all the created delegators will be using
the same mutable map, which is not thread-safe.





