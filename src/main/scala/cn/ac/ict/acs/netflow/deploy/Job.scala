package cn.ac.ict.acs.netflow.deploy

import cn.ac.ict.acs.netflow.deploy.JobType.JobType

object JobType extends Enumeration {
    type JobType = Value

    val ADHOC, REPORT, ONLINE = Value
}

/**
 *
 * @param id System wide unique id
 * @param tpe
 * @param first first launch time since epoch
 * @param interval
 * @param cmd
 */
case class Job(
    id: String,
    tpe: JobType,
    first: Long,
    interval: Option[Long],
    cmd: Command) { //TODO: should ONLINE query require much more resource?

}

case class JobDescription(
  jobName: Option[String],
  jobType: Option[String],
  deferTime: Option[RelativeTime],
  frequency: Option[RelativeTime],
  jobQuery: Option[JobQuery])

case class RelativeTime(num: Int, unit: String)

case class JobQuery(sql: String, functions: Seq[FuncDescription])

case class FuncDescription(name: String, inputPath: String, format: String)
