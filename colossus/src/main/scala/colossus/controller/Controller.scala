package colossus
package controller

import colossus.parsing.DataSize
import colossus.parsing.DataSize._
import core._

import scala.concurrent.duration.Duration

/**
 * Configuration for the controller
 *
 * @param outputBufferSize the maximum number of outbound messages that can be queued for sending at once
 * @param sendTimeout if a queued outbound message becomes older than this it will be cancelled
 * @param inputMaxSize maximum allowed input size (in bytes)
 * @param flushBufferOnClose
 */
case class ControllerConfig(
  outputBufferSize: Int,
  sendTimeout: Duration,
  inputMaxSize: DataSize = 1.MB,
  flushBufferOnClose: Boolean = true,
  metricsEnabled: Boolean = true
)

/**
 * This can be used to build connection handlers directly on top of the
 * controller layer
abstract class BasicController[I,O](
  val codec: Codec[O, I],
  val controllerConfig: ControllerConfig,
  val context: Context
) extends Controller[I,O]

 */

