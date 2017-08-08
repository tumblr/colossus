# About

### In a nutshell, what does Colossus do?

Colossus provides a lightweight framework for doing high-performance network
I/O on the JVM.  It handles things like setting up servers, managing
connections, and handling many of the low-level tasks required by applications.

### Should I use Colossus?

Colossus has been built primarily around the use case of creating stateless,
low-latency backend services.  While you can certainly build any kind of system
you want using it, if you're looking to build a web application or something
that is more likely to be CPU bound than IO bound, then Colossus will probably
not give you any major advantage over frameworks like Play! or Spray.

Colossus is ideal for I/O heavy applications that do a lot of "bit pushing".
Proxies, load balancers, databases, and data streams are all examples of
applications Colossus is well-suited for.

### How is Colossus different from Netty?

At a high level Colossus is similar to Netty.  Both frameworks use a pool of
worker threads, each with their own NIO event loop.  Colossus is different in
that it is tightly coupled to Akka, allowing near-seamless communication
between event-loops and actors.

Every component you can attach to an event-loop is also capable of hooking into
the worker actor's `receive` method, which opens the doors to mixed sync/async
code.

### How is Colossus different from Akka I/O?

The biggest difference is your code sits _much_ closer to the event loop than
with Akka I/O, which requires all your connection handling code to reside in
actors.  Being able to keep user-level code in-thread can result in
significantly less overhead for low-level operations.  With Akka I/O, simply
getting data from the wire to your code means sending it through several
actors.  With colossus, your code is literally only a couple function calls
away from NIO, which at very high loads makes a major difference in
performance.

Akka is generally very fast; the fact that Colossus is entirely built on Akka
speaks for that.  But in many cases the actor model is not the most ideal when
it comes to performance, and the small overhead of sending actor messages can
add up very quickly when millions of messages are in transit.  Therefore
Colossus is built on the premise than inter-actor communication is a
non-trivial operation and should be avoided unless it's actually necessary for
concurrency.

### Current State of Things

Colossus is currently in it's "pre-1.0" phase, meaning things are working, but
the framework in under very heavy development.  We're working very hard to make
Colossus as clean and usable as possible without making significant performance
sacrifices.


### Release Workflow

Currently we're following an accelerated release workflow.  Currently there is
no backwards compatibility between versions, but we have somewhat formalized
the release schedule to help everyone manage upgrading.  Releases are versioned with the scheme :

```
[MAJOR].[MINOR].[BUILD]
```

Right now the major version is fixed, minor versions introduce breaking
changes, and build versions aim to be backwards compatible (but may have small
breaking changes if there's a pressing need).  Eventually we'll move to a more
solid semantic versioning scheme where only major releases introduce breaking
changes


### License

Colossus is released under the [Apache License version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

This documentation is released under the [Creative Commons Attribution 3.0 Unported](http://creativecommons.org/licenses/by/3.0/) License. 


