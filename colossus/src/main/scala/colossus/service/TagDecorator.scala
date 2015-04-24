package colossus
package service

import metrics.TagMap

trait TagDecorator[I,O] {
  def tagsFor(request: I, response: O): TagMap
}

class DefaultTagDecorator[I,O] extends TagDecorator[I,O] {
  def tagsFor(request: I, response: O): TagMap = Map()
}

object TagDecorator {
  
  def default[I, O] = new DefaultTagDecorator[I,O]
}



trait TaggedResponse[T] { self: T =>
  private var tags: TagMap

  def addTag(key: String, value: String) {
    tags(key) = value
    this
  }
}



req: HttpRequest

req.ok("asdfas").withHeader(....).withTag("foo", "bar").withHeader()

Service.serve[Htt]

Service.serve[Completion, Http]

Serivce.serve[C, D, R]

C = Future | Callback | Identity

D = Completion | Identity

R = Http | ....



Request => Callback[Response] ==> Request => Callback[Identity[Response]]

Request => Callback[Completion[Response]]

Request => Future[Response]

Request => Future[Completion[Response]]
