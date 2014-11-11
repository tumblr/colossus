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

## Testing Core Servers

TODO

## Testing Services

The `ServiceSpec` class has some basic methods for doing integration testing on services.  You are required to provide two values:
 
* `service: ServerRef` : This is a running instance of your service, bound to `ServiceSpec.TestPort` (19999)
* `requestTimeout: FiniteDuration` : How long the matchers should wait for responses

Some helpful matchers are:

* `expectResponse(request: Request, response: Response)`
* `expectResponseType[T <: Response](request: Request)`

For http, the `HttpServiceSpec` contains some more helpful functions specifically for http.
