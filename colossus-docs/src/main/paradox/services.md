# Services

A service receives requests and returns responses. Colossus has built in support for http, memcache, redis and telnet.

## Http

A http service will take the following form:

@@snip [HttpServiceExample.scala](../scala/HttpServiceExample.scala) { #example }

`ok` is a helper method on `HttpRequest` that returns a `HttpResponse`. The implicit 
`colossus.service.Callback.Implicits.objectToSuccessfulCallback` then turns the response into a `Callback[HttpResponse]`.

The following helper method are available, which will default the content type to text and return the corresponding 
http code.

+ ok
+ notFound
+ error
+ badRequest
+ unauthorized
+ forbidden

There are several different ways to set headers on the response.

@@snip [HttpService2.scala](../scala/HttpService2.scala) { #example1 }

The content type header is a little special since it is usually set automatically but can also be set manually.
It can be set just like any other header.

@@snip [HttpService2.scala](../scala/HttpService2.scala) { #example1a }

You can also use `.withContent()` to set the content type header.
For a different HTTP status code, the helper methods don't exist; instead just use the `respond` method.

@@snip [HttpService2.scala](../scala/HttpService2.scala) { #example2 }

On the incoming request, body and content type are on the `HttpBody` and headers and parameters are on the 
`HttpHead`.

@@snip [HttpService2.scala](../scala/HttpService2.scala) { #example3 }

There is no built in middleware, but the same effect can be achieved by wrapping routes in functions. For example,
if you wanted easy access to the request body you might write:

@@snip [MiddlewareAsFunctions.scala](../scala/MiddlewareAsFunctions.scala) { #example }

And then use it like so:

@@snip [MiddlewareAsFunctions.scala](../scala/MiddlewareAsFunctions.scala) { #example1 }

You can add filters to modify and/or shortcut requests and responses.

Defining Filters:

@@snip [FilterExample.scala](../scala/FilterExample.scala) { #example }

To use them override `filters` function in your service.

@@snip [FilterExample.scala](../scala/FilterExample.scala) { #example1 }

To enable Gzip and Deflate compression just add HttpCustomFilters.CompressionFilter to the request handler filters.
@@snip[HttpCompressionExample.scala]($examples$/HttpCompressionExample.scala) {#compressed_http}

## Redis

A redis server will take the following form:

@@snip [RedisServiceExample.scala](../scala/RedisServiceExample.scala) { #example }

## Configuration

A service can be configured either programmatically or by using typesafe config. The settings in 
`colossus/src/resources/reference.conf` will be used as defaults.

To configure via config, create or update `application.conf` with the server specific settings:

```
colossus {
  service {
    example-server {
      request-timeout : 1 second
      request-buffer-size : 100
      log-errors : true
      request-metrics : true
      max-request-size : "1000 MB"
    }
  }
}
```

To configure via code, create a `ServiceConfig` object and pass it to the `RequestHandler`.

@@snip [ServiceConfigExample.conf](../scala/ServiceConfigExample.scala) { #example }

@@snip [ServiceConfigExample.conf](../scala/ServiceConfigExample.scala) { #example1 }

`RequestHandler` allows for the configuration of how request errors are reported. By default, `ColossusRuntimeException`s
are converted to `String`s and logged with no stack trace, and other exceptions are logged with a stack trace. A custom 
implementation of `RequestFormatter` can be provided as demonstrated in this example.

@@snip [ServiceConfigExample.conf](../scala/ServiceConfigExample.scala) { #example2 }
