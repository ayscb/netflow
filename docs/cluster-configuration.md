{:.table-name}表2 加载集群的配置
<table class="table">
  <tr>
  	<th style="width:21%">环境变量</th>
  	<th>代表的含义</th>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_MASTER_HOST</code></td>
  	<td>QueryMaster的启动IP</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_MASTER_PORT</code></td>
  	<td>QueryMaster的启动端口（默认是9099）</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_MASTER_WEBUI_PORT</code></td>
  	<td>QueryMaster的网页控制台端口（默认是18080）</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_MASTER_OPTS</code></td>
  	<td>仅作用在QueryMaster的System Property，以"-Dx=y"的方式给出（默认为空），有效的选项见表3</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_WORKER_CORES</code></td>
  	<td>设置该节点上可供Spark Application Driver使用的核数（默认为所有可用核）</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_WORKER_MEMORY</code></td>
  	<td>设置该节点上可分配给Spark Application Driver使用的内存总量（默认为可用内存总数减1GB）</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_WORKER_PORT</code></td>
  	<td>QueryWorker在该节点的启动端口（默认为随机值）</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_WORKER_WEBUI_PORT</code></td>
  	<td>【确认】QueryWorker在该节点的网页控制台端口（默认是18081）</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_WORKER_DIR</code></td>
  	<td>QueryWorker在该节点的工作目录（包括log和tmp）（默认是NETFLOW_HOME/work）</td>
  </tr>
  <tr>
  	<td><code>NETFLOW_QUERY_WORKER_OPTS</code></td>
  	<td>仅作用在QueryWorker的System Property，以"-Dx=y"的方式给出（默认为空），有效的选项见表4</td>
  </tr>
  <tr>
  	<td><code></code></td>
  	<td></td>
  </tr>
</table>

{:.table-name}表3 NETFLOW_QUERY_MASTER_OPTS的可选配置项
<table class="table">
  <tr>
  	<th style="width:21%">配置项</th>
  	<th>默认值（单位）</th>
  	<th>代表的含义</th>
  </tr>
  <tr>
  	<td><code>netflow.deploy.retainedQuerys</code></td>
  	<td>200（个）</td>
  	<td>网页上保留的已完成Query的最大个数，更早的Query会被从网页上剔除</td>
  </tr>
  <tr>
  	<td><code>netflow.queryWorker.timeout</code></td>
  	<td>60（秒）</td>
  	<td>QueryMaster在多长的时间间隔内未收到QueryWorker心跳时会认为该worker丢失</td>
  </tr>
</table> 

{:.table-name}表x 基于ZooKeeper的QueryMaster HA配置
<table class="table">
  <tr>
  	<th style="width:21%">配置项</th>
  	<th>代表的含义</th></tr>
  <tr>
    <td><code>netflow.deploy.recoveryMode</code></td>
    <td>设为『ZOOKEEPER』以开启热备QueryMaster恢复模式</td>
  </tr>
  <tr>
    <td><code>netflow.deploy.zookeeper.url</code></td>
    <td>ZooKeeper集群的URL（例如, 192.168.1.100:2181,192.168.1.101:2181）.</td>
  </tr>
  <tr>
    <td><code>netflow.deploy.zookeeper.dir</code></td>
    <td>Zookeeper用于存储可恢复状态的目录（默认为 /netflow-query）</td>
  </tr>
</table>