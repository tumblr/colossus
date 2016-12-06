package colossus
package core

import java.net.InetAddress

/**
 * This class is used to report some basic stats about a Connection.  This is used in conjunction
 * with the Metrics library and ConnectionPageRenderer.
 * @param domain The domain of this Connection
 * @param host The host of this Connection
 * @param id The id of this Connection
 * @param timeOpen The amount of time this Connection has been open
 * @param readIdle milliseconds since the last read activity on the connection
 * @param writeIdle milliseconds since the last write activity on the connection
 * @param bytesSent The amount of bytes that this Connection has sent
 * @param bytesReceived The amount of bytes that this Connection has received
 */
case class ConnectionSnapshot(
  domain: String, //either a server name or client
  host: InetAddress, //this is not a string because looking up the hostname takes time
  id: Long,
  timeOpen: Long = 0,
  readIdle: Long = 0,
  writeIdle: Long = 0,
  bytesSent: Long = 0,
  bytesReceived: Long = 0
) {

  def timeIdle = math.min(readIdle, writeIdle)

}

