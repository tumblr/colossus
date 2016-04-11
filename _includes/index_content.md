<h2>See it in Action</h2>

The obligatory 5 line Hello-world Service:

{% highlight scala %}

object Main extends App {
  
  implicit val system = IOSystem()

  Server.basic("example-server", 9000){ new HttpService(_) {
    def handle = {
      case request @ Get on Root => request.ok("Hello world!")      
      case request @ Get on Root / "echo" / str => request.ok(str)
    }
  }}
}

{% endhighlight %}

For a more substantial example, here's a simple http proxy using Memcached for caching:

{% highlight scala %}

object Main extends App {

  implicit val system = IOSystem()

  Server.start("proxy", 9000) { new Initializer(_) {

    val remoteHost     = Http.client("remote.host", 8080)
    val memcacheClient = Memcache.client("memcache.host", 11211)

    def onConnect = new HttpService(_) {

      def handle = {
        case request => memcacheClient.get(request.head.bytes).flatMap{
          case Some(responseBytes) => HttpResponse.fromBytes(responseBytes)
          case None => for {
            response    <- remoteHost.send(request)
            memcacheSet <- memcacheClient.set(request.head.bytes, response.bytes)
          } yield response
        }
      }
    }
  }

}

{% endhighlight %}

<h2>How It Works</h2>

Colossus is a low-level event-based framework.  In a nutshell, it spins up
multiple event loops, generally one per CPU core.  TCP connections are bound to
event loops and request handlers (written by you) are attached to connections to
transform incoming requests into responses.  I/O operations (such as making
requests to a remote cache or another service) can be done asynchronously and
entirely within the event loop, eliminating the need for complex multi-threaded
code and greatly reducing the overhead of asynchronous operations.



