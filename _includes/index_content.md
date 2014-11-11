<h2>Get Started Now!</h2>

Add the following to your build.sbt or Build.scala (2.10 or 2.11)

{% highlight scala %}
libraryDependencies += "com.tumblr" %% "colossus" % "{{ site.latest_version }}"
{% endhighlight %}

Now let's make a little server.

{% highlight scala %}
import colossus._
import service._
import protocols.telnet._

object Main extends App {
  
  implicit val io_system = IOSystem()

  Service.become[Telnet]("telnet-echo", 456){
    case TelnetCommand("exit" :: Nil) => TelnetReply("bye!").onWrite(Disconnect)
    case TelnetCommand("echo" :: text :: Nil) => TelnetReply(text)
  }
}
{% endhighlight %}



