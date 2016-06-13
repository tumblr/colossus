

trait StaticController[P <: Protocol] extends CoreHandler with ControllerImpl[P]{this: ControllerIface[P] =>


  private var _readsEnabled = true
  private var _writesEnabled = true
  def readsEnabled = _readsEnabled
  def writesEnabled = _writesEnabled

  private var outputBuffer new MessageQueue[Output](controllerConfig.outputBufferSize)

  override def connected(endpt: WriteEndpoint) {
    super.connected(endpt)
    codec.reset()
  }

  private def onClosed() {

  }

  protected def connectionClosed(cause : DisconnectCause) {
    onClosed()
  }

  protected def connectionLost(cause : DisconnectError) {
    onClosed()
  }

  private def signalWrite() {
    connectionState match {
      case a: AliveState => {
        if (writesEnabled) a.endpoint.requestWrite()
      }
      case _ => {}
    }
  }

  def receivedData(data: DataBuffer) {
    while (data.hasUnreadData) {
      codec.decode(data) match {
        case Some(m) => processMessage(m)
        case None => {}
      }
    }
  }

  //TODO: connectionstate
  def canPush = !outputBuffer.isFull

  def push(item: P#Output, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean = {
    if (canPush) {
      if (outputBuffer.isEmpty) signalWrite()
      outputBuffer.enqueue(item, postWrite, createdMillis)
      true
    } else false
  }

  def readyForData(buffer: DataOutBuffer) =  {
      

  

}
