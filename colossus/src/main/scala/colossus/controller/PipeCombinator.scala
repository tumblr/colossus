package colossus.controller

import java.util

import scala.util.{Try, Failure, Success}

object PipeCombinator {

  def join[SRCIN, SRCOUT, SNKIN, SNKOUT](input : Pipe[SRCIN, SRCOUT], output : Pipe[SNKIN, SNKOUT])(f : SRCOUT => Seq[SNKIN]) : Pipe[SRCIN, SNKOUT] = {

    new Pipe[SRCIN, SNKOUT] {

      import scala.collection.JavaConverters._

      private var queue : java.util.Queue[SNKIN] = new util.LinkedList[SNKIN]()

      pipeData()

      override def push(item: SRCIN): Try[PushResult] = input.push(item)

      override def pull(onReady: (Try[Option[SNKOUT]]) => Unit){ output.pull(onReady) }

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
          val results: Try[PushResult] = output.push(next)
          results match {
            case Success(PushResult.Done) => {
              input.complete()
            }
            case Success(PushResult.Full(t)) => {
              t.fill(pushToSink)
            }
            case Success(PushResult.Ok) => {
              pushToSink()
            }
            case Failure(a : PipeTerminatedException) => {
              terminatePipe(input, a.getCause)
            }
            case Failure(a : PipeClosedException) => {
              input.complete()
            }
            case Failure(a) => {
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

      //after this is called, data can no longer be written, but can still be read until EOS
      override def complete() {
        input.complete()
        output.complete()
      }
    }
  }
}
