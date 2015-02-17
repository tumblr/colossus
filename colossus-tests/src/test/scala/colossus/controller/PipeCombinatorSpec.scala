package colossus.controller

import org.scalatest.{TryValues, OptionValues, MustMatchers, WordSpec}

import scala.util.{Failure, Success}

class PipeCombinatorSpec extends WordSpec with MustMatchers with OptionValues with TryValues{

  "PipeCombinator" must {

    "combine 2 pipes with a map function" in {

      val writeSink: Pipe[String, String] = new InfinitePipe[String]
      val readSource: Pipe[Int, Int] = new InfinitePipe[Int]

      val map: (String) => Seq[Int] = (s: String) => s.split(":").map(_.toInt).toSeq

      val combined: Pipe[String, Int] = writeSink.join(readSource)(map)

      var callbackExecuted = false

      combined.fold(Seq[Int]())((el, ac) => ac :+ el).execute {
        case Success(x) => callbackExecuted = true; x must equal(Seq(1, 2, 3, 4, 5))
        case Failure(t) => throw t
      }
      callbackExecuted must be (false)
      combined.push("1:2:3") must equal(PushResult.Ok)
      combined.push("4:5") must equal(PushResult.Ok)
      combined.complete()

      callbackExecuted must be (true)
    }

    "completing a pipe will complete both sides" in {
      val writeSink: Pipe[String, String] = new InfinitePipe[String]
      val readSource: Pipe[Int, Int] = new InfinitePipe[Int]
      val map: (String) => Seq[Int] = (s: String) => s.split(":").map(_.toInt).toSeq
      val combined: Pipe[String, Int] = writeSink.join(readSource)(map)

      combined.complete()

      writeSink.push("") must equal(PushResult.Closed)

      readSource.pull {
        case Success(None) => {}
        case _ => throw new Exception("expected None")
      }
    }

    "terminating a pipe will terminate both sides" in {

      class GadZooksException(msg : String) extends Exception(msg)

      val writeSink: Pipe[String, String] = new InfinitePipe[String]
      val readSource: Pipe[Int, Int] = new InfinitePipe[Int]

      val map: (String) => Seq[Int] = (s: String) => s.split(":").map(_.toInt).toSeq

      val combined: Pipe[String, Int] = writeSink.join(readSource)(map)

      combined.terminate(new GadZooksException("snip snap"))

      val t = writeSink.push("")
      t mustBe a[PushResult.Error]


      readSource.pull {
        case Failure(ex : PipeTerminatedException) => {
          ex.getCause mustBe a[GadZooksException]
        }
        case other => throw new Exception(s"expected None, got $other")
      }
    }
  }

}
