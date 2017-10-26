package colossus.service

import colossus.service.GenRequestHandler.PartialHandler

abstract class Filter[P <: Protocol] {
  def apply(next: PartialHandler[P]): PartialHandler[P]
}
