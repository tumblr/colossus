---
layout: page
title:  "Workers"
categories: docs
---

Workers are the reactive event-loops that handle the service business logic and consist of an Initializer and Request Handler. Initializers are created for each worker and provide a place to share resources among all connections handled by the worker. It also provides access for updating data for the request handler and clients.

{% highlight scala %}

val server = Server.start("http-server", 8888) { workerRef =>
  new Initializer(workerRef) {
    var currentName = "Jones"

    override def receive = {
      case NameChange(name) => currentName = name
    }

    override def onConnect = new HttpService(_) {
      override def handle: PartialHandler[Http] = {
        case request @ Get on Root => 
          Callback.successful(request.ok(s"My name is $currentName"))
      }
    }
  }
}
  
{% endhighlight %}

Actor messages can then be broadcast to all workers using the `delegatorBroadcast` method on `ServerRef`.

{% highlight scala %}

server.delegatorBroadcast(NameChange("Smith"))

{% endhighlight %}