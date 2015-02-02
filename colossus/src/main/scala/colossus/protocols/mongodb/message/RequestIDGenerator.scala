package colossus.protocols.mongodb.message

import java.util.concurrent.atomic.AtomicInteger

object RequestIDGenerator {

  private val generator = new AtomicInteger(0)

  def generate: Int = generator.getAndIncrement
}
