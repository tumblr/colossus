import colossus.streaming._

object PipeExamples extends App {

  def basicPipe = {
    // #basic_pipe
    // The primary implementation of a Pipe is BufferedPipe, backed by a fixed-length buffer
    val pipe = new BufferedPipe[Int](5)

    //pushing to a Pipe returns a PushResult indicating if the push succeeded
    val pushResult: PushResult = pipe.push(2) //PushResult.Ok

    //pulling from a Pipe returns a PullResult
    val pullResult: PullResult[Int] = pipe.pull() //PullResult.Item(2)
    // #basic_pipe
  }

  def fullPush = {
    // #full_push
    //create a pipe with a buffer size of 1
    val pipe = new BufferedPipe[Int](1)

    pipe.push(10) //PushResult.ok

    //the pipe can only buffer one item, so the next push fails and returns a
    //PushResult.Full
    val fullResult = pipe.push(12).asInstanceOf[PushResult.Full]

    //provide a callback function for the returned signal
    fullResult.onReady.notify{
      println("ready to push")
    }

    //signal is triggered as soon as the item is pulled.  "ready to push" is
    //printed before pull() returns
    val item = pipe.pull()

    // #full_push  
  }

  def emptyPull = {
    // #empty_pull
    val pipe = new BufferedPipe[Int](10)

    //the pipe is empty so it returns a PullResult.Empty
    val result = pipe.pull().asInstanceOf[PullResult.Empty]

    //provide the returned signal with a callback function
    result.whenReady.notify {
      println("items available to pull")
    }

    //the signal is triggered as soon as an item is pushed into the pipe
    pipe.push(1)

    // #empty_pull

    
  }

}
