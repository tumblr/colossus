<h2>Get Started Now!</h2>

Add the following to your build.sbt or Build.scala (2.10 or 2.11)

{% highlight scala %}
libraryDependencies += "com.tumblr" %% "colossus" % "{{ site.latest_version }}"
{% endhighlight %}

Now let's make a little server.

{% highlight scala %}
import colossus._
import service._
import protocols.http._
import UrlParsing._
import HttpMethod._

object Main extends App {
  
  implicit val io_system = IOSystem()

  Service.become[Http]("http-echo", 9000){
    case request @ Get on Root => request.ok("Hello world!")
    case request @ Get on Root / "echo" / str => request.ok(str)
  }
}
{% endhighlight %}



