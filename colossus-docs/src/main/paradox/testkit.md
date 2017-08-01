# Testkit

Colossus has a test kit that includes some classes for simplifying tests for servers. Testkit is built on [ScalaTest](http://scalatest.org).

## Setup

Make sure you add `colossus-testkit` to your dependcies, using the same version as colossus itself.  For example:

```sbtshell
libraryDependencies += "com.tumblr" %% "colossus-testkit" % "LATEST_VERSION" % "test
```

## Unit Testing

### Testing Callbacks

`colossus.testkit.CallbackAwait` works similarly to Scala's `Await` for futures.  

### Testing Request Handlers

You can use `MockConnection` to create a fake `ServerContext` and create an instance of a request handler:

@@snip [TestkitExample.scala](../scala/TestkitExample.scala) { #example }

@@snip [TestkitExampleSpec.scala](../../test/scala/TestkitExampleSpec.scala) { #example }

## Integration Testing

`ColossusSpec` is a scalatest-based integration testing suite that contains a bunch of useful functions that help with 
spinning up instances of a service for testing

`withIOSystem(f: IOSystem => Unit)` will spin up a new `IOSystem` for the duration of a test and shut it down at the end.

`withServer(server: ServerRef)(f: => Unit)` will shutdown the given server after the test completes
