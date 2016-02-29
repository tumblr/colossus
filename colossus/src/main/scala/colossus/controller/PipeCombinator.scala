package colossus.controller

import java.util

import scala.util.{Try, Failure, Success}

object PipeCombinator {

  def join[SRCIN, SRCOUT, SNKIN, SNKOUT](input : Pipe[SRCIN, SRCOUT], output : Pipe[SNKIN, SNKOUT])(f : SRCOUT => Seq[SNKIN]) : Pipe[SRCIN, SNKOUT] = {

    new Pipe[SRCIN, SNKOUT] {

      import scala.collection.JavaConverters._

      private var queue  = new util.LinkedList[SNKIN]()

      pipeData()

      override def push(item: SRCIN): PushResult = input.push(item)

      override def pull(onReady: (Try[Option[SNKOUT]]) => Unit){ output.pull(onReady) }

      def isFull = input.isFull

      def pipeData() {
        input.pull {
          case Success(Some(data)) => {
            queue = new util.LinkedList[SNKIN](f(data).asJavaCollection) ///hrm...
            pushToSink()
          }
          case Success(None) => {
            output.complete()
          }

          case Failure(a : PipeTerminatedException) =>{
            terminatePipe(output, a.getCause)
          }
          case Failure(cause) => {
            terminatePipe(output, cause)
          }
        }
      }

      private def pushToSink() {
        if(queue.isEmpty){
          pipeData()
        }else{
          val next  = queue.remove()
          output.push(next) match {
            case PushResult.Complete => {
              input.complete()
            }
            case PushResult.Full(t) => {
              queue.addFirst(next)
              t.fill(pushToSink)
            }
            case PushResult.Filled(t) => {
              t.fill(pushToSink)
            }
            case PushResult.Ok => {
              pushToSink()
            }
            case PushResult.Closed => {
              //this should never happen
              terminatePipe(input, new Exception("Output Pipe unexpectedly closed"))
            }
            case PushResult.Error(a) => {
              terminatePipe(input, a)
            }
          }
        }
      }

      //can be triggered by either a source or sink, this will immediately cause
      override def terminate(reason: Throwable) {
        terminatePipe(input, reason)
        terminatePipe(output, reason)
      }

      private def terminatePipe[A, B](p : Pipe[A, B], t : Throwable) {
        if(!p.terminated) {
          p.terminate(t)
        }
      }


      override def terminated: Boolean = input.terminated && output.terminated
      def isClosed = input.isClosed && output.isClosed

      //after this is called, data can no longer be written, but can still be read until EOS
      override def complete() {
        input.complete()
        output.complete()
      }
    }
  }
}
