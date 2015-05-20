package cn.ac.ict.acs.netflow.master

import cn.ac.ict.acs.netflow.PersistenceEngine
import cn.ac.ict.acs.netflow.broker.Job

import scala.reflect.ClassTag

trait MasterPersistenceEngine extends PersistenceEngine {

  final def addBroker(broker: BrokerInfo): Unit = {
    persist("broker_" + broker.id, broker)
  }

  final def removeBroker(broker: BrokerInfo): Unit = {
    unpersist("broker_" + broker.id)
  }

  final def addJob(job: JobInfo): Unit = {
    persist("job_" + job.id, job)
  }

  final def removeJob(jobId: String): Unit = {
    unpersist("job_" + jobId)
  }

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   */
  final def readPersistedData(): (Seq[JobInfo], Seq[BrokerInfo]) = {
    (read[JobInfo]("job_"), read[BrokerInfo]("broker_"))
  }

}
