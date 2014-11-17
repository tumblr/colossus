---
layout: page
title:  "Actor Api"
categories: docs
---

## Introduction

As the name suggests, the actor API is a fully actor-based API on top of
Colossus core.  It allows you to assign an actor to every server or client
connection and interact with the connection purely through message passing.

Since it is built on the core layer, the actor API only works with raw TCP
connections.  Any higher-level abstraction needs to be implemented manually.

## Creating an actor-based server

When a server accepts a connection, two actors will be created.  The first is a
generic "proxy" actor that directly interacts with the connection and the event
loop, and the second actor is your own handler actor, which receives sanitized
messages from the proxy.

There are two sets of event messages that are passed between the handler and
proxy.  All messages from the proxy inherit the `ConnectionEvent` trait, and
all messages sent from the handler to the proxy are `ConnectionCommand`
objects.

Colossus will automatically register a newly created handler with its proxy.  Afterwards, the general flow of messages is:

1.  The proxy sends the `Connected` event to the handler.  For the handler, the `sender` of the message is the proxy, so hold onto that reference!
2.  The proxy begins to send `ReceivedData` events to the handler whenever it gets some data on the connection
3.  The handler sends `Write` commands to the proxy to write data.  With each write it can choose how writes are acknowledged
4.  The proxy sends a `WriteAck` message to the handler to indicate whether data was successfully written or not (it may fail if the write buffer is full)
5.  If the write buffer ever fills up, the proxy will send a `ReadyForData` event when the buffer has drained.
6.  The handler can terminate the connection by sending the `Disconnect` command, or by stopping itself or the proxy.

To create the server, first let's define a simple connection handler:

{% highlight scala %}

import ConnectionEvent._
import ConnectionCommand._

class Handler extends Actor {

  def receive = {
    case Connected => context.become(connected(sender))
  }

  def handleClosed: Receive = {
    case ConnectionTerminated(cause : DisconnectError) => {
      log.error(s"Connection unepexpectedly terminated: $cause")
    }
    case ConnectionTerminated(cause) => {
      log.info("Connection closed")
      context stop self
    }

  }

  def connected(con: ActorRef): Receive = handleClosed orElse {
    case ReceivedData(data) => con ! Write(data, AckLevel.Failure)
    case WriteAck(status) => context.become(waiting(con))
  }

  def waiting(con: ActorRef) : Receive = handleClosed orElse {
    case ReadyForData => context.become(connected(con))
  }

}

{% endhighlight %}

This example has a basic setup to handle connection termination and write
backpressure.  One actor will be created for every connection, and the actor
will immediately received the `Connected` message.  When the connection is
closed for any reason, a `ConnectionTerminated` message is sent.  At that
point, the proxy connection is already terminated.

Now, we simply need to start a server with a `Props` for the actor

*note - this api doesn't exist yet, make the delegator yourself >:(*

{% highlight scala %}

val config = ServerConfig(/*...*/)

val server = ActorServer(config, Props[Handler])

{% endhighlight %}

Building a client is very similar, except instead of starting a server, you
send a `Connect` message to the IO System.  Furthermore, clients may receive a
`ConnectionFailed` message if the connection fails.  Colossus will not
automically retry to connect, and you should simply create a new actor to try
again.

### Write Backpressure

When working with a connection in an asynchronous manner, handling write
backpressure is important.  The underlying NIO buffer can only hold so much
data, and if you attempt to write more data than it can handle, only part or
none will actually be written.  The actor API lets you handle this with write
acknowledgement.  The `Write` message contains an `ackLevel` field that lets
you specify how the write is acknowledged.  The possible write statuses are:

* **Complete** : The entire ByteString was written
* **Partial** : Only part of the data was written, but colossus is internally holding onto the rest and will automatically write it when able.  Any further writes will fail until that happens
* **Zero** : The buffer is full and no data was written or buffered
* **Failed** : There is a problem with the connection, no data was written or buffered

The available levels for write acknowledgement are :

* **None** : Writes are never acknowledged
* **Success** : Only the `Complete` and `Partial` statuses are sent
* **Failure** : Only the `Zero` and `Failed` statuses are sent
* **All** : Every write sends an acknowledgement

So if you send a write command with an ack level of `Failure`, and the data is
only partially written, you will then receive a `WriteAck(WriteStatus.Partial)`
message.

The safest way to write data is to use `AckLevel.All` and only send one write
at a time, waiting for the acknowledgement each time.  However, for
higher-throughput, you can use only `AckLevel.Failure`, but be aware that you
may end up receiving multiple failure messages.

*TODO - need to add tokens to writes so in the acks the user can know which writes failed*

## Creating an actor-based client
