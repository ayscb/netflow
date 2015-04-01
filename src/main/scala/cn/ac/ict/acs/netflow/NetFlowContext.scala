package cn.ac.ict.acs.netflow

class NetFlowContext {

  def submitQuery(query: Query): QueryResult = {

    null
  }

  def submitAdhoc(query: AdhocQuery): QueryResult = ???

  def registerReport(query: ReportQuery): Unit = ???

  def startReport(name: String): Unit = ???

  def stopReport(name: String): Unit = ???

  def registerOnline(query: OnlineQuery): Unit = ???

  def startOnline(name: String): Unit = ???

  def stopOnline(name: String): Unit = ???


}
