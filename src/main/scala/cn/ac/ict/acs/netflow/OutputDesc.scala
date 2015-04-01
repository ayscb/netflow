package cn.ac.ict.acs.netflow

sealed abstract class OutputDesc

case class Direct(schema: String) extends OutputDesc
case class File(path: String) extends OutputDesc
case class Socket(address: String) extends OutputDesc

class QueryResult