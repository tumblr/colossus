package colossus.streaming

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
class Trigger extends Signal{

  private var callbacks = new java.util.LinkedList[() => Unit]

  def empty = callbacks.size == 0

  def notify(cb: => Unit) {
    callbacks.add(() => cb)
  }

  def trigger(): Boolean = {
    if (callbacks.size == 0) false else {
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
