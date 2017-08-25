package colossus.service

import colossus.metrics.TagMap

trait TagDecorator[P <: Protocol] {
  def tagsFor(request: P#Request, response: P#Response): TagMap
}

class DefaultTagDecorator[P <: Protocol] extends TagDecorator[P] {
  def tagsFor(request: P#Request, response: P#Response): TagMap = Map()
}

object TagDecorator {

  def default[P <: Protocol] = new DefaultTagDecorator[P]
}
