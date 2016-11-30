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

