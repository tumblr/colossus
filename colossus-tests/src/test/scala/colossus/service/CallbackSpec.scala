package colossus
package service

import testkit._
import akka.testkit.TestProbe

import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

class CallbackSpec extends ColossusSpec {
  val func = {f: (Try[Int] => Unit) => f(Success(5))}

  class AE extends Exception("ASDF")
  class BE extends Exception("HHAA")

  "Callback" must {
    "basic callback" in {
      var value = 0
      val callback = Callback(func).map{i => value = i}
      callback.execute()
      value must equal(5)
    }
    "map" in {
      var value1 = 0
      var value2 = 0
      val cb = Callback(func).map{i => value1 = i;i+1}.map{j => value2=j}.execute()
      value1 must equal(5)
      value2 must equal(6)
    }
    "flatmap unmapped callback" in {
      var value: String = ""
      def sfunc(s: String): Callback[String] = Callback{f: (Try[String] => Unit) => f(Success("hey " + s))}
      val cb1 = UnmappedCallback(func)
      val cb2 = cb1.flatMap{i: Int => sfunc((i + 1).toString)}
      val cb3 = cb2.map{s => value = s}
      cb3.execute()
      value must equal("hey 6")
    }
    "flatmap mapped callback" in {
      var value: String = ""
      def sfunc(s: String): Callback[String] = Callback{f: (Try[String] => Unit) => f(Success("hey " + s))}
      val cb1: MappedCallback[Int, Int] = MappedCallback(func, _.map{_ + 1})
      val cb2 = cb1.flatMap{i: Int => sfunc((i + 1).toString)}
      val cb3 = cb2.map{s => value = s}
      cb3.execute()
      value must equal("hey 7")

    }

    "execute completion" in {
      var completed = false
      Callback(func).execute{
        case Success(i) => completed = true
        case Failure(err) => throw err
      }
      completed must equal(true)

      var failed = false
      Callback.failed(new AE).execute{
        case Failure(e: AE) => failed = true
        case _ => throw new Exception("Didn't fail?!?!")

      }
      failed must equal(true)
    }

    "sequence" in {
      var value1 = 0
      var value2 = 0
      var value3 = 0
      val cb1 = Callback(func).map{i => value1 = i + 2;value1}
      val cb2 = Callback(func).map{j => value2 = j + 4;value2}
      val seq = Callback.sequence(List(cb1, cb2)).map{vals => value3 = vals.map{_.getOrElse(0)}.sum}
      seq.execute()
      value1 must equal(7)
      value2 must equal(9)
      value3 must equal(16)
    }
    "empty sequence" in {
      var value1 = false
      val emptySeq = Callback.sequence(List()).map{i => value1 = true }
      emptySeq.execute()
      value1 must equal(true)
    }
    "successful" in {
      var value1 = 0
      val c = Callback.successful(5).map{i => value1 = i}
      c.execute()
      value1 must equal(5)
    }
    "failed" in {
      var value1 = 0
      val c = Callback.failed(new Exception("XXX")).map{i => value1 = i}
      c.execute()
      value1 must equal(0)
    }


    "fromFuture" in {
      import system.dispatcher
      val p = Promise[String]()
      val t = TestProbe()
      var res = ""
      implicit val executor = CallbackExecutor(t.ref)
      val ec = implicitly[ExecutionContext]
      val c = Callback.fromFuture(p.future)
      c.map{x => res = x.toUpperCase}.execute()
      t.expectNoMsg(100.milliseconds)
      p.complete(Success("hello"))
      val ex = t.receiveOne(50.milliseconds) match {
        case e: CallbackExec => e
        case _ => throw new Exception("wrong type")
      }
      ex.execute()
      res must equal("HELLO")
    }

    "schedule" in {
      import system.dispatcher
      val t = TestProbe()
      implicit val executor = CallbackExecutor(t.ref)
      var res = ""
      var res2 = ""
      val cb1 = Callback(func).map{i => res = (i + 2).toString;res}

      val c = Callback.schedule(500.milliseconds)(cb1).map{r => res2 = "HI " + r}
      c.execute()
      val x = t.receiveOne(50.milliseconds) match {
        case e: CallbackExec => e
        case _ => throw new Exception("wrong type")
      }
      x.in must equal (Some(500.milliseconds))
      x.execute()
      res must equal("7")
      res2 must equal("HI 7")
    }

    "catch exception in map" in {
      var j = false
      //from unmapped callback
      val c = Callback(func).map[Int]{i => throw new Exception("HEY")}.map{_ => j = true}.execute()
      j must equal(false)

      //from mapped callback
      var a = false
      var b = false
      val c1 = Callback(func).map{x => a = true;x}.map{i => throw new Exception("LISTEN")}.map{_ => b = true}.execute()
      a must equal(true)
      b must equal(false)
    }

    "catch exception in flatMap" in {
      var j = false
      //from unmapped callback
      val c = Callback(func).flatMap[Int]{i => throw new Exception("HEY")}.map{_ => j = true}.execute()
      j must equal(false)

      //from mapped callback
      var a = false
      var b = false
      val c1 = Callback(func).map{x => a = true;x}.flatMap{i => throw new Exception("LISTEN")}.map{_ => b = true}.execute()
      a must equal(true)
      b must equal(false)

      //double flatmap
      //from unmapped callback
      val d = Callback(func).flatMap[Int]{i => throw new Exception("HEY")}.flatMap{i => throw new Exception("B")}.map{_ => j = true}.execute()
    }

    "failure is propagated through maps and flatMaps" in {
      var res: Option[Try[Int]] = None
      val x = Callback.failed(new Exception("HEY")).flatMap{_ => Callback.successful(3)}.flatMap{i => Callback.successful(i)}.execute {r =>
        res = Some(r)
      }

      res.get mustBe a[Failure[_]]


    }


    "recover" in {
      var recovered = false
      val c = Callback(func).map[Int]{i => throw new Exception("OVER HERE")}.recover{
        case t: Throwable => 6
      }.map{i => recovered = true}.execute()
      recovered must equal (true)
    }

    "recover after map" in {
      var recovered = false
      var mapped = false
      Callback(func).map[Int]{i => throw new Exception("WATCH OUT")}.map{_ => mapped = true}.recover{
        case t => recovered = true
      }.execute()
      recovered must equal(true)
      mapped must equal(false)
    }

    "recover after flatMap" in {
      var recovered = false
      var mapped = false
      Callback(func).flatMap[Int]{i => throw new Exception("WATCH OUT")}.map{_ => mapped = true}.recover{
        case t => recovered = true
      }.execute()
      recovered must equal(true)
      mapped must equal(false)

    }

    "recover - fall-through" in {
      var recovered = false
      Callback(func).map[Int]{i => throw new AE}.recover{
        case e: BE => 4
      }.map{h => recovered = true}.execute()
      recovered must equal(false)
    }

    "failure in recovery" in {
      var caught = false
      Callback(func).map[Int]{i => throw new AE}.recover{
        case a: AE => throw new BE
      }.execute{
        case Failure(b: BE) => caught = true
        case _ => {}
      }
      caught must equal(true)
    }

    "recoverWith (mapped)" in {
      var recovered = false
      var executedBad = false
      var executedGood = false
      Callback(func).map[Int]{i => throw new Exception("LOOK")}.map{i => executedBad = true}.recoverWith{
        case t: Throwable => Callback(func).map{i => recovered = true}
      }.map{j => executedGood = true}.execute()
      recovered must equal(true)
      executedBad must equal(false)
      executedGood must equal(true)
    }

    "failure in recoverWith" in {
      var caught = false
      Callback(func).map[Int]{i => throw new AE}.recoverWith{
        case a: AE => throw new BE
      }.execute{
        case Failure(b: BE) => caught = true
        case _ => {}
      }
      caught must equal(true)
    }


    "recoverWith (unmapped)" in {
      var recovered = false
      var executedGood = false
      val failfunc: (Try[String] => Unit) => Unit = f => f(Failure(new Exception("HELLO")))
      Callback(failfunc).recoverWith{
        case t: Throwable => Callback(func).map{i => recovered = true}
      }.map{j => executedGood = true}.execute()
      recovered must equal(true)
      executedGood must equal(true)
    }

    "zip2" in {
      val a = Callback(func).map{i => "a"}
      val b = Callback(func).map{i => "b"}
      var ok = false
      a.zip(b).execute{
        case Success(("a", "b")) => ok = true
        case _ => {}
      }
      ok must equal(true)
    }

    "zip3" in {
      val a = Callback(func).map{i => "a"}
      val b = Callback(func).map{i => "b"}
      val c = Callback(func).map{i => "c"}
      var ok = false
      a.zip(b, c).execute{
        case Success(("a", "b", "c")) => ok = true
        case _ => {}
      }
      ok must equal(true)
    }

    "zip does not sequentially chain callbacks" in {
      var triggered1 = false
      var triggered2 = false
      //notice these functions don't actually call the handler functions, so if the callbacks are chained, the second will never execute
      //eg, the original implementation (in the companion obj) was
      // def zip(a,b) = a.flatMap{res_a => b.map{res_b => (res_a, res_b)}}
      // which works, execpt there's no concurrency, b doesn't even execute until a finishes
      val func1: (Try[Int] => Unit) => Unit = {x => triggered1 = true}
      val func2: (Try[Int] => Unit) => Unit = {x => triggered2 = true}
      Callback(func1).zip(Callback(func2)).execute()
      triggered1 must equal(true)
      triggered2 must equal(true)

    }

    "filter" in {
      val fail = Callback(func).withFilter{_ > 12345}
      val succeed = Callback(func).map{i => 999999}.withFilter{_ > 12345}
      var fr: Option[Try[Int]] = None
      var sr: Option[Try[Int]] = None
      fail.execute{res => fr = Some(res)}
      succeed.execute{res => sr = Some(res)}

      fr.get.isSuccess must equal(false)
      sr must equal(Some(Success(999999)))


    }

    "toFuture" in {
      import scala.concurrent.ExecutionContext.Implicits.global
      var value = 0
      val callback = Callback(func).map { i => i }
      value = Await.result(callback.toFuture, 2 seconds)
      value must equal(5)
    }

    "toFuture failed" in {
      import scala.concurrent.ExecutionContext.Implicits.global

      val badfunc = {f: (Try[Int] => Unit) => f(Failure[Int](new Exception))}
      val callback = Callback(badfunc).map { i => i }
      val result = Try(Await.result(callback.toFuture, 1 seconds))
      result.isFailure must equal(true)
    }

    "not suppress exceptions thrown in the execute block" in {
      //in practice, throwing an exception here is really bad, since the
      //exception will likely not actually be thrown at the call site, so any
      //try/catch around .execute will not actually catch the exception.  But
      //this is better than simply suppressing the exception, which is what
      //used to happen
      //
      //also note, this is only an issue in the final block passed to execute.
      //In any other place, even when mapping/flatMapping, unhandled exceptions are
      //properly caught inside the callback and suppressed, allowing for
      //recovery using recover and recoverWith.  The difference is this final
      //block has no way to recover, so we have to either suppress or throw
      //without the possibility of recovery.
      //todo: maybe completely eliminate the block in execution

      class FoobarException extends Exception("FOOBAR")

      intercept[CallbackExecutionException] {
        Callback(func).execute{
          case _ => throw new FoobarException
        }
      }

      intercept[CallbackExecutionException] {
        Callback(func).flatMap{x => Callback(func)}.execute {
          case _ => {
            val e = new FoobarException
            //e.printStackTrace()
            throw e
          }
        }
      }

    }

  }

  "ConstantCallback" must {

    "catch exception thrown during mapTry" in {
      ConstantCallback(Success(4)).mapTry{
        case _ => throw new Exception("Fail")
      }.execute{
        case Success(_) => fail("this should never occur")
        case Failure(t) => {}
      }
    }

    "catch exception thrown during map" in {
      ConstantCallback(Success(4)).map{i =>
        throw new Exception("Fail")
      }.execute{
        case Success(_) => fail("this should never occur")
        case Failure(t) => {}
      }
    }

    "catch exception thrown during flatMap" in {
      ConstantCallback(Success(4)).flatMap{i =>
        throw new Exception("Fail")
      }.execute{
        case Success(_) => fail("this should never occur")
        case Failure(t) => {}
      }
    }
  }


  "CallbackPromise" must {
    "execute when it gets a value" in {
      var res = 0
      val c = new CallbackPromise[Int]()
      c.callback.map{i => res = i + 1}.execute()
      res must equal(0)
      c.success(5)
      res must equal(6)
    }

    "complete immediately when executed and a value is present" in {
      var res = 0
      val c = new CallbackPromise[Int]()
      c.success(5)
      c.callback.map{i => res = i + 1}.execute()
      res must equal(6)
    }
  }
}
