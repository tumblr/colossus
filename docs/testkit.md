---
layout: page
title:  "Testing Servers"
categories: docs
---

Colossus has a test kit that includes some classes for simplifying tests for servers.  Testkit is built on [ScalaTest](http://scalatest.org).

## Setup

Make sure you add `colossus-testkit` to your dependcies, using the same version as colossus itself.  For example:

{% highlight scala %}
libraryDependencies += "com.tumblr" %% "colossus" % "{{ site.latest_version }}"

libraryDependencies += "com.tumblr" %% "colossus-testkit" % "{{ site.latest_version }}" % "test"
{% endhighlight %}


## Unit Testing

### Testing Callbacks

`colossus.testkit.CallbackAwait` works similarly to Scala's `Await` for futures.  

### Testing Request Handlers

You can use `MockConnection` to create a fake `ServerContext` and create an instance of a request handler:

{% highlight scala %}

class MyHandler(context: ServerContext) extends Service[Http](context) {
  def handle = {
    case req => req.ok("hello")
  }
}

"request handler" must {
  "generate a response" in {
    val connection = MockConnection.server(new MyHandler(_))
    val response = connection.typedHandler.handle(HttpRequest.get("/foo"))
    CallbackAwait(response).body.bytes.utf8String mustBe "hello"
  }
}
{% endhighlight %}

## Integration Testing

`ColossusSpec` is a scalatest-based integration testing suite that contains a
bunch of useful functions that help with spinning up instances of a service for
testing

`withIOSystem(f: IOSystem => Unit)` will spin up a new `IOSystem` for the duration of a test and shut it down at the end.

`withServer(server: ServerRef)(f: => Unit)` will shutdown the given server after the test completes
