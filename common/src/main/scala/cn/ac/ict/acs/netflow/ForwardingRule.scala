package cn.ac.ict.acs.netflow

case class ForwardingRule(rules: Seq[RuleItem])

case class RuleItem(routerId: String, srcPort: Int, destIp: String, rate: String)
