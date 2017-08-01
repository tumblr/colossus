# Workers

Workers are the reactive event-loops that handle the service business logic and consist of an Initializer and Request 
Handler. Initializers are created for each worker and provide a place to share resources among all connections handled 
by the worker. It also provides access for updating data for the request handler and clients. Actor messages can then 
be broadcast to all workers using the `initializerBroadcast` method on `ServerRef`.

@@snip [WorkerExample.scala](../scala/WorkerExample.scala) { #example }
