package cn.ac.ict.acs.netflow

class QueryDesc(val queryType: QueryType, val frequency: Interval) {
  require((queryType == Adhoc && frequency.once) ||
    ((queryType == Report || queryType == Online) && !frequency.once))
}

sealed abstract class QueryType
case object Adhoc extends QueryType
case object Report extends QueryType
case object Online extends QueryType

/**
 * @param time Interval in seconds
 */
case class Interval(time: Long) {
  require(time >= 0)

  def once = time == 0
}

object Interval {

  val _minute = 60
  val _hour = _minute * 60
  val _day = _hour * 24
  val _week = _day * 7

  def minutes(m: Int) = new Interval(_minute * m)
  def hours(h: Int) = new Interval(_hour * h)
  def days(d: Int) = new Interval(_day * d)
  def weeks(w: Int) = new Interval(_week * w)

  def once() = new Interval(0)

}