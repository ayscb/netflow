package cn.ac.ict.acs.netflow

case class Subnet(start: Int, end: Int, name: String) {

  def map(target: Int): String =
    if (target <= end) name else null
}

class SubnetMapping {

}
