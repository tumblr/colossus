---
layout: page
title:  "Overview"
categories: docs
---

**Colossus** is a lightweight, event-driven I/O layer for building Scala services.

Colossus is built around a few core principles:

1. **Keep it simple** - Reduce boilerplate and barrier to entry.  Writing a service should be as easy as writing a single function!
2. **Keep it fast** - Colossus aims to have as little overhead as possible between NIO and your code.
3. **Keep it transparent** - Choose how much magic you want!  Colossus is built in several layers which allow you to build high-level abstractions without being walled off from taking full control.
4. **Keep it focused** - Colossus has a relatively small codebase and is not intended to be an overreaching framework.  It is not meant to solve all your back-end "webscale" architectural problems, but rather solve a specific subset of those problems very well.

## Concepts

Colossus is an **event-based** framework, which means all of your code runs
inside a single-threaded *event-loop*.  Actually that's only half of the truth;
Colossus scales by running multiple identical event loops in parallel, with
each loop taking a portion of connections handled by your server.  However, a
single connection is permanently bound to exactly one event loop for its
lifetime, so code dealing with a single connection is effectively
single-threaded.  This is a fairly common method of high-scale I/O called the
*multi-reactive model*.

Colossus is **non-blocking**, meaning that whenever a connection attached to
your server isn't doing anything, it is not blocking any threads, keeping the
event loop free for other connections that do have work to do.  This means that
even a Colossus server running just one event loop has no problem
simultaneously handling thousands of connections.

Colossus is also **actor-based**, meaning that the event loops are actually actors.
This lets us work around what is normally the biggest challenge of event-based
systems, interacting with blocking and parallel code.  So if your service has to
interact with a blocking API, perform some CPU intensive operation, or perform
some task in parallel with the event loop, Colossus makes it easy to jump out
of the event loop using actors and futures.  This also makes it easy to safely
share state between event loops and connections.


## Architecture

Colossus is built mostly in four modules:

#### Core

This is a fully generalized multi-reactor implementation on top of NIO.  The
Core layer manages event loops, servers and raw TCP connections, as well as
providing the API for being able to attach event handlers to incoming server
connections and outgoing client connections.

#### Service

This layer has all the logic for building RPC-like services that transform
request objects into response objects.  Common issues like managing
backpressure are handled by the service layer.

#### Protocols

Colossus comes with implementations for several application-level protocols
packaged into Codecs.  While codecs are currently only used within services,
they are fully decoupled from the service layer, making them usable in other
situations that don't have the same semantics as services.

#### Util

Mostly empty at the moment.  The biggest component is the [Task]({{ site.baseurl }}/docs/tasks) API
which lets you run arbitrary code inside event loops.

