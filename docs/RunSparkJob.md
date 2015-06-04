(use http://websequencediagrams.com/ to visualize sequence diagrams)

整体流程
=======

RequestHandler->QueryMaster: RestRequestSubmitJob(jobDesc)

note over QueryMaster: Validate Request

opt if request is invalid
  QueryMaster->RequestHandler: 400
end

    note over QueryMaster
      Create JobInfo
      Persist in ZK
    end note
   
QueryMaster->RequestHandler: 202 + jobId
    
QueryMaster->Scheduler: Register Job
Scheduler->QueryMaster: RunJob(jobId)
QueryMaster->SparkMaster: SubmitJob(JobInstance)
SparkMaster->+JobActor: Launch Job
JobActor->QueryMaster: JobStarted(jobId)

opt if job failed
  JobActor->QueryMaster: JobFailed(jobId)
end

opt if job is adhoc
  JobActor->JobResultTracker: JobResult(jobId, result)
end

JobActor->QueryMaster: JobFinished(jobId)