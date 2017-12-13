package colossus.service

abstract class Interceptor[P <: Protocol] {
  def apply(request: P#Request, callback: Callback[P#Response] => Callback[P#Response])
    : (P#Request, Callback[P#Response] => Callback[P#Response])
}
