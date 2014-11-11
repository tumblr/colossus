package colossus.metrics

import net.liftweb.json._
import JsonDSL._

case class Frame(timestamp: Long, metrics: MetricMap)

object Frame {
  def empty() = Frame(System.currentTimeMillis, MetricMap.Empty)
}

case class Window(frames: Seq[Frame]) {

  def filter(filter: MetricFilter) = Window(
    frames.map{frame =>
      Frame(frame.timestamp, frame.metrics.filter(filter))
    }
  )

  def timeSeries(address: MetricAddress, tags: TagMap): TimeSeries = {
    val data: Seq[(Long, Long)] = frames.flatMap{frame =>
      frame.metrics.get(address).flatMap{values => 
        values.get(tags).map{value => (frame.timestamp, value)}
      }
    }
    TimeSeries(address, tags, data)
  }

  def allTimeSeries(normalize: Boolean): Seq[TimeSeries] = {
    var max = 0
    var min = Long.MaxValue
    val series = collection.mutable.ArrayBuffer[TimeSeriesBuilder]()
    frames.foreach{frame => 
      frame.metrics.fragments.foreach{fragment =>
        series.find{_.matches(fragment.address, fragment.tags)}.map{s =>
          s.add(frame.timestamp, fragment.value)
        }.getOrElse{
          val b =  new TimeSeriesBuilder(fragment.address, fragment.tags)
          series += b
          b.add(frame.timestamp, fragment.value)
        }
      }
    }
    series.map{_.build}
  }

  def allTimeSeries: Seq[TimeSeries] = allTimeSeries(false)
}

class TimeSeriesBuilder(address: MetricAddress, tags: TagMap) {
  val data = collection.mutable.ArrayBuffer[(Long, Long)]()
  def matches(addr: MetricAddress, tgs: TagMap) = addr == address && tags == tgs
  def add(timestamp: Long, value: Long) {
    data += (timestamp -> value)
  }
  def build = TimeSeries(address, tags, data.toList)
}

//map is timestamp -> value
case class TimeSeries(address: MetricAddress, tags: TagMap, data: Seq[(Long, Long)]) {

  def + (timestamp: Long, value: Long) = copy(data = data :+ (timestamp, value))
  def values = data.map{_._2}
  def toJson = ("address" -> address.toString) ~ ("data" -> data.map{_._2}.toList)

  def toHighChartSeries = ("id" -> tags.id) ~ ("name" -> tags.name) ~ ("data" -> data.map{t => List(t._1, t._2)}.toList)
}



