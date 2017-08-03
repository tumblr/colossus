@@@ index

* [Quick Start](quickstart.md)
* [Clients](clients.md)
* [Workers](workers.md)
* [Services](services.md)
* [Configuration](configuration.md)
* [Testing Servers](testkit.md)
* [Metrics](metrics.md)
* [Tasks](tasks.md)
* [About](about.md)

@@@

# Colossus

The obligatory 5 line Hello-world Service:

@@snip [HelloWorld.scala](../scala/HelloWorld.scala) { #hello_world_example }

Colossus is a low-level event-based framework. In a nutshell, it spins up multiple event loops, generally one per CPU 
core. TCP connections are bound to event loops and request handlers (written by you) are attached to connections to
transform incoming requests into responses.  I/O operations (such as making requests to a remote cache or another 
service) can be done asynchronously and entirely within the event loop, eliminating the need for complex multi-threaded
code and greatly reducing the overhead of asynchronous operations.

## Scala docs

* [Colossus API](colossus-api/index.html#colossus.package)
* [Colossus Metrics API](colossus-metrics-api/index.html#colossus.package)
* [Colossus Teskit API](colossus-testkit-api/index.html#colossus.package)
