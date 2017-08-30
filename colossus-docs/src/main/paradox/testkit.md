# Testkit

Colossus has a test kit that includes some classes for simplifying tests for servers. Testkit is built on [ScalaTest](http://scalatest.org).

## Setup

Make sure you add `colossus-testkit` to your dependencies, using the same version as colossus itself.  For example:

```sbtshell
libraryDependencies += "com.tumblr" %% "colossus-testkit" % "LATEST_VERSION" % "test
```

## Unit Testing

### Testing Callbacks

`colossus.testkit.CallbackAwait` works similarly to Scala's `Await` for futures.  

### Testing Request Handlers / Integration Testing

Request handlers are created in tests exactly as they would be in actual services.

@@snip [TestkitExample.scala](../scala/TestkitExample.scala) { #example }

To test the behavior of the request handler, we use `HttpServiceSpec`.
This is an extension of `ColossusSpec`, which is a scalatest-based integration testing suite containing some useful functions
that create one-off instances of a service for testing.

`withIOSystem(f: IOSystem => Unit)` will spin up a new `IOSystem` for the duration of a test and shut it down at the end.

`withServer(server: ServerRef)(f: => Unit)` will shutdown the given server after the test completes.

`HttpServiceSpec` adds to `ColossusSpec` some HTTP-specific utility functions for asserting that your handler behaves correctly.
To illustrate, we first start with a slightly modified version of the previous `MyHandler` request handler:

@@snip [TestkitExampleSpec.scala](../../test/scala/TestkitExampleSpec.scala) { #example1 }

And now, the `HttpServiceSpec`-based test class:

@@snip [TestkitExampleSpec.scala](../../test/scala/TestkitExampleSpec.scala) { #example2 }

`expectCodeAndBody()` will hit the service instance with a request,
and match the returned code and body with a given expected code and body.

However, if the body is somewhat complex (e.g. JSON) and might contain things you don't care about like indentation, spacing, or even ordering,
it may not be efficient or even meaningful to match a given body directly.

`expectCodeAndBodyPredicate()` is a recent addition that, instead of taking a string that the body must match,
takes a predicate that the body must satisfy. In the example, the returned JSON body is parsed into a `Map[String, JValue]`
and is tested for equality against a given `Map[String, JValue]` instead.

