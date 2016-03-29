package colossus.service

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import colossus.IOSystem
import colossus.core.WorkerRef


/**
 * A Sender is anything that is able to asynchronously send a request and
 * receive a corresponding response
 */
trait Sender[C <: Protocol, M[_]] {

  def send(input: C#Input): M[C#Output]

  def disconnect()

}


/**
 * A Typeclass for abstracting over callbacks and futures
 */
trait Async[M[_]] {
  
  //the environment type is really only needed by futures (the execution context
  //when mapping and flatmapping)
  type E


  implicit def environment: E

  def map[T, U](t : M[T])(f : T => U)  : M[U]

  def flatMap[T, U](t : M[T])(f : T => M[U]) : M[U]

  def success[T](t : T) : M[T]

  def failure[T](ex : Throwable) : M[T]

}

trait AsyncBuilder[M[_], E] {

  def build(env: E): Async[M]
}

object AsyncBuilder {

  implicit object CallbackAsyncBuilder extends AsyncBuilder[Callback, WorkerRef] {
    def build(w: WorkerRef) = CallbackAsync
  }

  implicit object FutureAsyncBuilder extends AsyncBuilder[Future, IOSystem] {
    def build(i: IOSystem) = new FutureAsync()(i)
  }
}


object Async {

  implicit def cbAsync: Async[Callback] = CallbackAsync

  implicit def fAsync(implicit io: IOSystem) : Async[Future] = new FutureAsync

}

object CallbackAsync extends Async[Callback] {
  
  type E = Unit //we don't actually need the workerRef, but having the same environment var as the constructor for the base client works nicely

  implicit val environment: E = ()

  def map[T, U](t: Callback[T])(f: (T) => U): Callback[U] = t.map(f)

  def flatMap[T, U](t: Callback[T])(f: T => Callback[U]): Callback[U] = t.flatMap(f)

  def success[T](t: T): Callback[T] = Callback.successful(t)

  def failure[T](ex: Throwable): Callback[T] = Callback.failed(ex)
}

class FutureAsync(implicit val environment: IOSystem) extends Async[Future] {

  type E = IOSystem

  import environment.actorSystem.dispatcher

  def map[T, U](t: Future[T])(f: (T) => U): Future[U] = t.map(f)

  def flatMap[T, U](t: Future[T])(f: T => Future[U]): Future[U] = t.flatMap(f)

  def success[T](t: T): Future[T] = Future.successful(t)

  def failure[T](ex: Throwable): Future[T] = Future.failed(ex)
}

