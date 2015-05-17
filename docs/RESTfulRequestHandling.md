(use http://websequencediagrams.com/ to visualize sequence diagrams)

整体流程
=======

user->HttpListener: 连接请求

HttpListener->+HttpServerConnection: 转发请求
  note over HttpServerConnection: 将请求打包成HttpRequest
  
HttpServerConnection->RestBroker: HttpRequest
  note over RestBroker
  HttpRequest模式匹配
  转为对应的Message
  end note
  
RestBroker->+RequestHandler: Dispatch Message
RequestHandler->QueryMaster: RequestMessage
QueryMaster->RequestHandler: ResponseMessage
RequestHandler-->-HttpServerConnection: 将ResponseMessage转化为HttpResponse
HttpServerConnection-->user: 将HttpResponse转化成Response



用户请求和QueryMaster应答
==================

_仅包含QueryMaster成功响应的流程_

HttpServerConnection->RestBroker: HttpRequest

RestBroker-> +RequestHandler: 分派HttpRequest

opt if /status && GET
  RequestHandler->QueryMaster: RestRequestQueryMasterStatus
  QueryMaster-> RequestHandler: RestResponseQueryMasterStatus(...)
end

opt if /v1/jobs && GET
  RequestHandler->QueryMaster: RestRequestAllJobsInfo
  QueryMaster-> RequestHandler: RestResponseAllJobsInfo(...)
end

opt if /v1/jobs && POST
  RequestHandler->QueryMaster: RestRequestSubmitJob(jobDesc)
  QueryMaster-> RequestHandler: RestResponseSubmitJobSuccess(...)
end

opt if /v1/jobs/<jobId> && GET
  RequestHandler->QueryMaster: RestRequestJobInfo(jobId)
  QueryMaster-> RequestHandler: RestResponseJobInfo(...)
end

opt if /v1/jobs/<jobId> && DELETE
  RequestHandler->QueryMaster: RestRequestKillJob(jobId)
  QueryMaster-> RequestHandler: RestResponseKillJobSuccess(...)
end