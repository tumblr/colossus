package colossus.controller

trait Encoding {
  type Input
  type Output
}

object Encoding {
  type Apply[I,O] = Encoding { type Input = I; type Output = O }
}
