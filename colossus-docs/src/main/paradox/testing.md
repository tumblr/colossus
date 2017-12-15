# Testing

Colossus comes with a testkit that contains some helper classes and functions that simplifying the testing of a colossus 
service. Testkit is built on [ScalaTest](http://scalatest.org). Add `colossus-testkit` to your dependencies, using the 
same version as colossus itself. For example:

```sbtshell
libraryDependencies += "com.tumblr" %% "colossus-testkit" % "VERSION" % Test
```

## Testing Callbacks

@extref[CallbackAwait](docs-testkit:colossus.testkit.CallbackAwait$) works similarly to Scala's `Await` for futures.

@@snip [CallbackExampleSpec.scala](../../test/scala/CallbackExampleSpec.scala) { #example }

## Testing any Service

@extref[ServiceSpec](docs-testkit:colossus.testkit.ServiceSpec) abstract class provides helper functions to test expected 
responses. To use `ServerSpec` an implementation of the `service` method needs to be provided. In this example we are 
testing want to test this redis handler, but it could be any protocol:

@@snip [RedisServiceExample.scala](../scala/RedisServiceExample.scala) { #example2 }

Unit tests could look like:

@@snip [AnyServiceSpec.scala](../../test/scala/AnyServiceSpec.scala) { #example }

`ServerSpec` extends `ColossusSpec`, which provides the ability to spin up a server/iosystem. These functions can be used
directly for more flexible testing needs:

* `withIOSystem` will spin up a new `IOSystem` for the duration of a test and shut it down at the end.
* `withServer` will shutdown the given server after the test completes.

## Testing HTTP Service

`HttpServiceSpec` adds to `ServerSpec` some HTTP-specific utility functions for asserting that a HTTP handler behaves correctly.
To illustrate, let's test this request handler:

@@snip [MyHttpHandlerSpec.scala](../../test/scala/MyHttpHandlerSpec.scala) { #example1 }

And now, the `HttpServiceSpec`-based test class:

@@snip [MyHttpHandlerSpec.scala](../../test/scala/MyHttpHandlerSpec.scala) { #example2 }

* `expectCodeAndBody` will hit the service instance with a request, and match the returned code and body with a given
expected code and body.
* `expectCodeAndBodyPredicate` takes a predicate that the body must satisfy. In the example, the returned JSON body 
is parsed into a `Map[String, JsonNode]` and is tested for equality against a given `Map[String, JsonNode]` instead.

## Testing Clients

Colossus clients should be passed into the request handler so they can be mocked in tests. For example, lets test this
code:

@@snip [AnyClientExample.scala](../scala/AnyClientExample.scala) { #example }

`MockSender` functionality can be used in the following manner:

@@snip [AnyClientSpec.scala](../../test/scala/AnyClientSpec.scala) { #example1 }

Alternatively, using mockito, the clients can be mocked:

@@snip [AnyClientSpec.scala](../../test/scala/AnyClientSpec.scala) { #example2 }

## Testing Metrics

At its core, metrics are just a collection of maps containing strings and numbers. To test metrics, get the map and 
make sure it contains what you expect. Let's say we have this code:

@@snip [SimpleMetricExample.scala](../scala/SimpleMetricExample.scala) { #example }

The test could be written like this:

@@snip [SimpleMetricExampleSpec.scala](../../test/scala/SimpleMetricExampleSpec.scala) { #example }

