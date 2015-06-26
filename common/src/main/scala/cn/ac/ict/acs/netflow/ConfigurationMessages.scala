package cn.ac.ict.acs.netflow

trait ConfigurationMessage extends Serializable

object ConfigurationMessages {

  case object GetAllRules extends ConfigurationMessage

  case class CurrentRules(availableRules: Array[(String, RuleItem)]) extends ConfigurationMessage

  case class InsertRules(rules: ForwardingRule)
    extends ConfigurationMessage

  case class InsertionSuccess(insertNum: Int) extends ConfigurationMessage

  case class UpdateSingleRule(ruleId: String, ruleItem: RuleItem)
    extends ConfigurationMessage

  case class SingleRuleSubstitution(oldItem: Option[RuleItem], newItem: RuleItem)
    extends ConfigurationMessage

  case class DeleteSingleRule(ruleId: String) extends ConfigurationMessage

  case class DeletedRule(deletedRule: Option[RuleItem]) extends ConfigurationMessage
}
