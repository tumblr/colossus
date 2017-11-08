package colossus.service

import java.net.InetSocketAddress

import scala.concurrent.{Future, Promise}
import scala.language.higherKinds
import colossus.IOSystem
import colossus.core.WorkerRef

import scala.concurrent.duration.FiniteDuration

/**
  * A Sender is anything that is able to asynchronously send a request and
  * receive a corresponding response
  */
trait Sender[P <: Protocol, M[_]] {

  def send(input: P#Request): M[P#Response]

  def disconnect()
  
  def addInterceptor(interceptor: Interceptor[P]): Unit

  def address(): InetSocketAddress

}

/**
  * A Typeclass for abstracting over callbacks and futures
  */
trait Async[M[_]] {

  //the environment type is really only needed by futures (the execution context
  //when mapping and flatmapping)
  type E

  implicit def environment: E

  def map[T, U](t: M[T])(f: T => U): M[U]

  def flatMap[T, U](t: M[T])(f: T => M[U]): M[U]

  def success[T](t: T): M[T]

  def failure[T](ex: Throwable): M[T]

  def recoverWith[T, U >: T](t: M[T])(p: PartialFunction[Throwable, M[U]]): M[U]

  def delay[T](delay: FiniteDuration)(block: => M[T]): M[T]

  def execute[T](t: M[T]): Unit
}

/**
  * A Typeclass for building Async instances, used internally by ClientFactory.
  * This is needed to get the environment into the Async.
  */
trait AsyncBuilder[M[_], E] {

  def build(env: E): Async[M]
}

object AsyncBuilder {

  implicit object CallbackAsyncBuilder extends AsyncBuilder[Callback, WorkerRef] {
    def build(w: WorkerRef) = new CallbackAsync(w.callbackExecutor)
  }

  implicit object FutureAsyncBuilder extends AsyncBuilder[Future, IOSystem] {
    def build(i: IOSystem) = new FutureAsync(i)
  }
}

class CallbackAsync(val callbackExecutor: CallbackExecutor) extends Async[Callback] {

  type E = CallbackExecutor

  implicit val environment: E = callbackExecutor

  def map[T, U](t: Callback[T])(f: (T) => U): Callback[U] = t.map(f)

  def flatMap[T, U](t: Callback[T])(f: T => Callback[U]): Callback[U] = t.flatMap(f)

  def success[T](t: T): Callback[T] = Callback.successful(t)

  def failure[T](ex: Throwable): Callback[T] = Callback.failed(ex)

  def recoverWith[T, U >: T](t: Callback[T])(p: PartialFunction[Throwable, Callback[U]]): Callback[U] = t.recoverWith(p)

  def delay[T](delay: FiniteDuration)(block: => Callback[T]): Callback[T] =
    Callback.schedule(delay)(block)

  override def execute[T](t: Callback[T]): Unit = {
    t.execute()
  }
}

class FutureAsync(val environment: IOSystem) extends Async[Future] {

  type E = IOSystem

  import environment.actorSystem.dispatcher

  def map[T, U](t: Future[T])(f: (T) => U): Future[U] = t.map(f)

  def flatMap[T, U](t: Future[T])(f: T => Future[U]): Future[U] = t.flatMap(f)

  def success[T](t: T): Future[T] = Future.successful(t)

  def failure[T](ex: Throwable): Future[T] = Future.failed(ex)

  def recoverWith[T, U >: T](t: Future[T])(p: PartialFunction[Throwable, Future[U]]): Future[U] = t.recoverWith(p)

  def delay[T](delay: FiniteDuration)(block: => Future[T]): Future[T] = {
    val promise = Promise[T]
    environment.actorSystem.scheduler.scheduleOnce(delay) {
      val whenItGoesWell = flatMap(block) { t =>
        promise.success(t).future
      }
      recoverWith(whenItGoesWell) {
        case throwable =>
          promise.failure(throwable).future
      }
    }
    promise.future
  }

  override def execute[T](t: Future[T]): Unit = {
    Unit
  }
}
