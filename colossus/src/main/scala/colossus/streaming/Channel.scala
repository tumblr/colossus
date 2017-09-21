package colossus.streaming

class Channel[I, O](sink: Sink[I], source: Source[O]) extends Pipe[I, O] {

  def push(item: I): PushResult = sink.push(item)

  def pull() = source.pull()
  def peek   = source.peek

  def outputState = source.outputState
  def inputState  = sink.inputState

  def complete() = sink.complete()

  override def pullWhile(fn: O => PullAction, onc: TerminalPullResult => Any) {
    source.pullWhile(fn, onc)
  }

  override def pullUntilNull(fn: O => Boolean): Option[NullPullResult] = source.pullUntilNull(fn)

  def pushPeek = sink.pushPeek

  //TODO: This works fine when the termination is done on the channel, but what
  //happens if either the source or sink is independantly terminated?  Perhaps
  //we have to add a termination hook or something, or perhaps half-terminated channels don't matter?
  def terminate(reason: Throwable) {
    sink.terminate(reason)
    source.terminate(reason)
  }

}

object Channel {

  def apply[I, O](bufferSize: Int = 10): (Channel[I, O], Channel[O, I]) = {
    val inpipe  = new BufferedPipe[I](bufferSize)
    val outpipe = new BufferedPipe[O](bufferSize)
    (new Channel(inpipe, outpipe), new Channel(outpipe, inpipe))
  }

}
