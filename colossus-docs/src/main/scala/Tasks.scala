import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.core.ProxyActor.Receive
import colossus.task.Task

object Tasks extends App {

  // #example
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  Task.start(context =>
    new Task(context) {

      override def run() {
        //do your stuff here
      }

      override def receive: Receive = {
        case _ => // receive messages here
      }
  })
  // #example

}
