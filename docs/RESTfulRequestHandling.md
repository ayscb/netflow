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
  RequestHandler->QueryMaster: RestQueryMasterStatusRequest
  QueryMaster-> RequestHandler: RestQueryMasterStatusResponse(...)
end

opt if /v1/jobs && GET
  RequestHandler->QueryMaster: RestAllJobsInfoRequest
  QueryMaster-> RequestHandler: RestAllJobsInfoResponse(...)
end

opt if /v1/jobs && POST
  RequestHandler->QueryMaster: RestSubmitJobRequest(jobDesc)
  QueryMaster-> RequestHandler: RestSubmitJobSuccessResponse(...)
end

opt if /v1/jobs/<jobId> && GET
  RequestHandler->QueryMaster: RestJobInfoRequest(jobId)
  QueryMaster-> RequestHandler: RestJobInfoResponse(...)
end

opt if /v1/jobs/<jobId> && DELETE
  RequestHandler->QueryMaster: RestKillJobRequest(jobId)
  QueryMaster-> RequestHandler: RestKillJobSuccessResponse(...)
end