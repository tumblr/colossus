package colossus.controller

import colossus.service.Protocol

trait Encoding {
  type Input
  type Output
}

object Encoding {
  type Apply[I, O] = Encoding { type Input = I; type Output = O }

  type Server[P <: Protocol] = Encoding { type Input = P#Request; type Output  = P#Response }
  type Client[P <: Protocol] = Encoding { type Input = P#Response; type Output = P#Request }
}
