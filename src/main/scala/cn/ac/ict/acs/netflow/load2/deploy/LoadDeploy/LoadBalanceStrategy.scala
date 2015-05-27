package cn.ac.ict.acs.netflow.load2.deploy.loadDeploy

/**
 * balance the load worker
 * between multiple consumer threads ( resolveNetFlow ) and a single Producer
 * Created by ayscb on 2015/5/6.
 */
trait LoadBalanceStrategy {
  def loadBalanceWorker()
}