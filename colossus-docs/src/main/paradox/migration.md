# Migration Guide

# 0.10.x to 0.11.x

+ Package namespace changes, a non-exhausted list:
    + `colossus.protocols.http.server.HttpServer` => `colossus.protocols.http.HttpServer`
    + `colossus.protocols.http.server.Initializer` => `colossus.protocols.http.Initializer`
    + `colossus.protocols.http.server.RequestHandler` => `colossus.protocols.http.RequestHandler`
    + `colosuss.metrics.LoggerSender` => `colosuss.metrics.senders.LoggerSender`
    + `colosuss.metrics.OpenTsdbSender` => `colosuss.metrics.senders.OpenTsdbSender`
+ Content type removed from `HttpBody` and is now correctly accessible as a `HttpHeader`. For example:

    ```scala
    request.ok(HttpBody("text", HttpHeader("content-type", "application/json")))
    ```
    
    Becomes
    
    ```scala
    request.ok(HttpBody("text")).withContentType(ContentType.ApplicationJson)
    ```
