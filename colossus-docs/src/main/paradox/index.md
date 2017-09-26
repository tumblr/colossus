@@@ index

* [Quick Start](quickstart.md)
* [Clients](clients.md)
* [Workers](workers.md)
* [Services](services.md)
* [Configuration](configuration.md)
* [Testing Servers](testkit.md)
* [Metrics](metrics.md)
* [Tasks](tasks.md)
* [Streaming](streaming.md)
* [About](about.md)

@@@

# Colossus

The obligatory Hello-world Service:

@@snip [HelloWorld.scala](../scala/HelloWorld.scala) { #hello_world_example }

Colossus is a low-level event-based framework. In a nutshell, it spins up multiple event loops, generally one per CPU 
core. TCP connections are bound to event loops and request handlers (written by you) are attached to connections to
transform incoming requests into responses.  I/O operations (such as making requests to a remote cache or another 
service) can be done asynchronously and entirely within the event loop, eliminating the need for complex multi-threaded
code and greatly reducing the overhead of asynchronous operations.

## Source & Docs

* [https://github.com/tumblr/colossus](https://github.com/tumblr/colossus)
* @extref[Colossus API](docs:colossus.package)
* @extref[Colossus Metrics API](docs-metrics:colossus.package)
* @extref[Colossus Teskit API](docs-testkit:colossus.package)
