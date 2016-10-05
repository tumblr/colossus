package colossus.streaming


class Channel[I,O](sink: Sink[I], source: Source[O]) extends Pipe[I,O] {

  def push(item: I): PushResult = sink.push(item)

  def pull() = source.pull() 

  def outputState = source.outputState
  def inputState = sink.inputState

  def complete() = sink.complete()


  //TODO: This works fine when the termination is done on the channel, but what
  //happens if either the source or sink is independantly terminated?  Perhaps
  //we have to add a termination hook or something, or perhaps half-terminated channels don't matter?
  def terminate(reason: Throwable) {
    sink.terminate(reason)
    source.terminate(reason)
  }

}

object Channel {

  def apply[I,O]() : (Channel[I,O], Channel[O,I]) = {
    val inpipe = new BufferedPipe[I](10)
    val outpipe = new BufferedPipe[O](10)
    (new Channel(inpipe, outpipe), new Channel(outpipe, inpipe))
  }

}
