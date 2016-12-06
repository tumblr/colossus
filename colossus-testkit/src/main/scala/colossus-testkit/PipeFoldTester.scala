package colossus
package testkit

import akka.util.ByteString
import core.DataBuffer
import service.Callback
import scala.util.{Try, Success, Failure}


/**
 * This can be used to test folding on pipes. {{{
 val pipe = new InfinitePipe[Int, Int]
 val foldDB = pipe.fold(0){(a, b) => a + b}
 val tester = new FoldTester(foldCB)
 pipe.push(3)
 pipe.push(4)
 pipe.complete()
 tester.expect(7)
 }}}
 */
class PipeFoldTester[T](foldCB: Callback[T]) {
  private var res: Option[Try[T]] = None
  foldCB.execute{result => res = Some(result)}

  private def expectRes(r: Try[T] => Unit) {
    res.map(r).getOrElse{
      throw new Exception("Callback did not complete")
    }
  }

  def expect(expected: T) {
    expectRes{
      case Success(v) => if (v != expected) throw new Exception(s"Callback expected $expected, got $v")
      case Failure(err) => throw new Exception(s"Expected $expected, but callback failed with $err")
    }
  }

  def expectFailure() {
    expectRes{
      case Success(v) => throw new Exception("Expected Callback failure, but it completed with $v")
      case Failure(_) => {}
    }
  }

  def expectIncomplete() {
    res.foreach{value =>
      throw new Exception(s"Expected incomplete folding, but folding has completed with value $value")
    }
  }

}

object PipeFoldTester {

  def byteFolder(buffer: DataBuffer, build: ByteString) = build ++ ByteString(buffer.takeAll)


}
