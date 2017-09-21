package colossus.streaming

/**
  * A `Signal` is a callback mechanism used by both [[Source]] and [[Sink]] to manage forward/back-pressure.  In both cases it is returned when a requested operation cannot immediately complete, but can at a later point in time.  For example, when pulling from a [[Source]], if no item is immediately available, a signal is returned that will be triggered when an item is available to pull.
  * {{{
  * val stream: Source[Int] = //...
  * stream.pull() match {
  *  case PullResult.Item(num) => //...
  *  case PullResult.Full(signal) => signal.notify {
  *    //when this callback function is called, it is guaranteed that an item is now available
  *    stream.pull()//...
  *  }
  *}
 }}}
  * Signals are multi-listener, so that multiple consumers can attach callbacks
  * to a single listener.  Callbacks are fired in the order they are queued, and
  * generally conditions for triggering a signal are re-checked for each listener
  * (so that, for example, if one item is pushed to an empty Source, only one
  * listener is signaled).
  */
trait Signal {
  def notify(cb: => Unit)
}

/**
  * When a user attempts to push a value into a pipe, and the pipe either fills
  * or was already full, a Trigger is returned in the PushResult.  This is
  * essentially just a fillable callback function that is called when the pipe
  * either becomes empty or is closed or terminated
  *
  * Notice that when the trigger is executed we don't include any information
  * about the state of the pipe.  The handler can just try pushing again to
  * determine if the pipe is dead or not.
  */
class Trigger extends Signal {

  private var callbacks = new java.util.LinkedList[() => Unit]

  def empty = callbacks.size == 0

  def notify(cb: => Unit) {
    //println("adding signal")
    callbacks.add(() => cb)
  }

  def trigger(): Boolean = {
    if (callbacks.size == 0) false
    else {
      callbacks.remove()()
      true
    }
  }

  def triggerAll() {
    while (trigger()) {}
  }

  def clear() {
    callbacks.clear()
  }

}
