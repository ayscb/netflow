package cn.ac.ict.acs.netflow.metrics

import org.scalatest.{BeforeAndAfter, FunSuite}

class MetricsConfigSuite extends FunSuite with BeforeAndAfter {

  var filePath: String = _

  before {
    filePath = getClass.getClassLoader.getResource("test_metrics_config.properties").getFile
  }

  test("functionality") {
    val mc = new MetricsConfig(Option(filePath))
    mc.initialize()
    mc
  }



}
