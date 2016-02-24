<h2>Get Started Now!</h2>

Add the following to your build.sbt or Build.scala (2.10 or 2.11)

{% highlight scala %}
libraryDependencies += "com.tumblr" %% "colossus" % "{{ site.latest_version }}"
{% endhighlight %}

Here's a simple example service.

{% highlight scala %}

object Main extends App {
  
  implicit val io_system = IOSystem()

  Server.basic("example-server", 9000){ new HttpService(_) {
      
    def handle = {
      case request @ Get on Root => 
        Callback.successful(request.ok("Hello world!"))
      
      case request @ Get on Root / "echo" / str => 
        Callback.successful(request.ok(str))
    }
  }}
}

{% endhighlight %}



