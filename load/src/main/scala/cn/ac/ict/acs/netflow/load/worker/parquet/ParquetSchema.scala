/**
 * Copyright 2015 ICT.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load.worker.parquet

import org.apache.parquet.schema.{MessageType, Type, PrimitiveType}
import org.apache.parquet.schema.Type.Repetition._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

import cn.ac.ict.acs.netflow.{ Logging, NetFlowException }

object ParquetSchema extends Logging {

  private val headerFields = new Array[Type](5)
  headerFields(0) = new PrimitiveType(REQUIRED, INT64, "time")
  headerFields(1) = new PrimitiveType(OPTIONAL, BINARY, "router_ipv4")
  headerFields(2) = new PrimitiveType(OPTIONAL, BINARY, "router_ipv6")
  headerFields(3) = null
  headerFields(4) = null

  private val bgpFields = new Array[Type](9)
  bgpFields(0) = new PrimitiveType(OPTIONAL, BINARY, "router_prefix")
  bgpFields(1) = new PrimitiveType(OPTIONAL, BINARY, "router_ipv4")
  bgpFields(2) = new PrimitiveType(OPTIONAL, BINARY, "router_ipv6")
  bgpFields(3) = new PrimitiveType(OPTIONAL, BINARY, "next_hop_ipv4")
  bgpFields(4) = new PrimitiveType(OPTIONAL, BINARY, "next_hop_ipv6")
  bgpFields(5) = new PrimitiveType(OPTIONAL, BINARY, "as_path")
  bgpFields(6) = new PrimitiveType(OPTIONAL, BINARY, "community")
  bgpFields(7) = new PrimitiveType(OPTIONAL, BINARY, "adjacent_as")
  bgpFields(8) = new PrimitiveType(OPTIONAL, BINARY, "self_as")

  private val netflowFields = new Array[Type](101)
  netflowFields(0) = null
  // 1-10
  netflowFields(1) = new PrimitiveType(OPTIONAL, INT64, "in_bytes")
  netflowFields(2) = new PrimitiveType(OPTIONAL, INT64, "in_pkts")
  netflowFields(3) = new PrimitiveType(OPTIONAL, INT64, "flows")
  netflowFields(4) = new PrimitiveType(OPTIONAL, INT32, "protocol")
  netflowFields(5) = new PrimitiveType(OPTIONAL, INT32, "src_tos")
  netflowFields(6) = new PrimitiveType(OPTIONAL, INT32, "tcp_flags")
  netflowFields(7) = new PrimitiveType(OPTIONAL, INT32, "l4_src_port")
  netflowFields(8) = new PrimitiveType(OPTIONAL, BINARY, "ipv4_src_addr")
  netflowFields(9) = new PrimitiveType(OPTIONAL, INT32, "src_mask")
  netflowFields(10) = new PrimitiveType(OPTIONAL, INT64, "input_snmp")

  // 11-20
  netflowFields(11) = new PrimitiveType(OPTIONAL, INT32, "l4_dst_port")
  netflowFields(12) = new PrimitiveType(OPTIONAL, BINARY, "ipv4_dst_addr")
  netflowFields(13) = new PrimitiveType(OPTIONAL, INT32, "dst_mask")
  netflowFields(14) = new PrimitiveType(OPTIONAL, INT64, "output_snmp")
  netflowFields(15) = new PrimitiveType(OPTIONAL, BINARY, "ipv4_next_hop")
  netflowFields(16) = new PrimitiveType(OPTIONAL, INT64, "src_as")
  netflowFields(17) = new PrimitiveType(OPTIONAL, INT64, "dst_as")
  netflowFields(18) = new PrimitiveType(OPTIONAL, BINARY, "bgp_ipv4_next_hop")
  netflowFields(19) = new PrimitiveType(OPTIONAL, INT64, "mul_dst_pkts")
  netflowFields(20) = new PrimitiveType(OPTIONAL, INT64, "mul_dst_bytes")

  // 21-30
  netflowFields(21) = new PrimitiveType(OPTIONAL, INT64, "last_switched")
  netflowFields(22) = new PrimitiveType(OPTIONAL, INT64, "first_switched")
  netflowFields(23) = new PrimitiveType(OPTIONAL, INT64, "out_bytes")
  netflowFields(24) = new PrimitiveType(OPTIONAL, INT64, "out_pkts")
  netflowFields(25) = new PrimitiveType(OPTIONAL, INT32, "min_pkt_lngth")
  netflowFields(26) = new PrimitiveType(OPTIONAL, INT32, "max_pkt_lngth")
  netflowFields(27) = new PrimitiveType(OPTIONAL, BINARY, "ipv6_src_addr")
  netflowFields(28) = new PrimitiveType(OPTIONAL, BINARY, "ipv6_dst_addr")
  netflowFields(29) = new PrimitiveType(OPTIONAL, INT32, "ipv6_src_mask")
  netflowFields(30) = new PrimitiveType(OPTIONAL, INT32, "ipv6_dst_mask")

  // 31-40
  netflowFields(31) = new PrimitiveType(OPTIONAL, INT32, "ipv6_flow_label") // ipv6 only use 20bit
  netflowFields(32) = new PrimitiveType(OPTIONAL, INT32, "icmp_type")
  netflowFields(33) = new PrimitiveType(OPTIONAL, INT32, "mul_igmp_type")
  netflowFields(34) = new PrimitiveType(OPTIONAL, INT64, "sampling_interval")
  netflowFields(35) = new PrimitiveType(OPTIONAL, INT32, "sampling_algorithm")
  netflowFields(36) = new PrimitiveType(OPTIONAL, INT32, "flow_active_timeout")
  netflowFields(37) = new PrimitiveType(OPTIONAL, INT32, "flow_inactive_timeout")
  netflowFields(38) = new PrimitiveType(OPTIONAL, INT32, "engine_type")
  netflowFields(39) = new PrimitiveType(OPTIONAL, INT32, "engine_id")
  netflowFields(40) = new PrimitiveType(OPTIONAL, INT64, "total_bytes_exp")

  // 41-50
  netflowFields(41) = new PrimitiveType(OPTIONAL, INT64, "total_pkts_exp")
  netflowFields(42) = new PrimitiveType(OPTIONAL, INT64, "total_flows_exp")
  netflowFields(43) = null // *Vendor Proprietary
  netflowFields(44) = new PrimitiveType(OPTIONAL, INT64, "ipv4_src_prefix")
  netflowFields(45) = new PrimitiveType(OPTIONAL, INT64, "ipv4_dst_prefix")
  netflowFields(46) = new PrimitiveType(OPTIONAL, INT32, "mpls_top_label_type")
  netflowFields(47) = new PrimitiveType(OPTIONAL, BINARY, "mpls_top_label_ip_addr")
  netflowFields(48) = new PrimitiveType(OPTIONAL, INT32, "flow_sampler_id")
  netflowFields(49) = new PrimitiveType(OPTIONAL, INT32, "flow_sampler_mode")
  netflowFields(50) = new PrimitiveType(OPTIONAL, INT64, "flow_sampler_random_interval")

  // 51-60
  netflowFields(51) = null // *Vendor Proprietary
  netflowFields(52) = new PrimitiveType(OPTIONAL, INT32, "min_ttl")
  netflowFields(53) = new PrimitiveType(OPTIONAL, INT32, "max_ttl")
  netflowFields(54) = new PrimitiveType(OPTIONAL, INT32, "ipv4_ident")
  netflowFields(55) = new PrimitiveType(OPTIONAL, INT32, "dst_tos")
  netflowFields(56) = new PrimitiveType(OPTIONAL, BINARY, "in_src_mac")
  netflowFields(57) = new PrimitiveType(OPTIONAL, BINARY, "out_dst_mac")
  netflowFields(58) = new PrimitiveType(OPTIONAL, INT32, "src_vlan")
  netflowFields(59) = new PrimitiveType(OPTIONAL, INT32, "dst_vlan")
  netflowFields(60) = new PrimitiveType(OPTIONAL, INT32, "ip_protocol_version")

  // 61-70
  netflowFields(61) = new PrimitiveType(OPTIONAL, INT32, "direction")
  netflowFields(62) = new PrimitiveType(OPTIONAL, BINARY, "ipv6_next_hop")
  netflowFields(63) = new PrimitiveType(OPTIONAL, BINARY, "bpg_ipv6_next_hop")
  netflowFields(64) = new PrimitiveType(OPTIONAL, INT64, "ipv6_option_headers")
  netflowFields(65) = null // *Vendor Proprietary
  netflowFields(66) = null // *Vendor Proprietary
  netflowFields(67) = null // *Vendor Proprietary
  netflowFields(68) = null // *Vendor Proprietary
  netflowFields(69) = null // *Vendor Proprietary
  netflowFields(70) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_1") // contain 20bit

  // 71-80
  netflowFields(71) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_2") // contain 20bit
  netflowFields(72) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_3") // contain 20bit
  netflowFields(73) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_4") // contain 20bit
  netflowFields(74) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_5") // contain 20bit
  netflowFields(75) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_6") // contain 20bit
  netflowFields(76) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_7") // contain 20bit
  netflowFields(77) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_8") // contain 20bit
  netflowFields(78) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_9") // contain 20bit
  netflowFields(79) = new PrimitiveType(OPTIONAL, INT32, "mpls_label_10") // contain 20bit
  netflowFields(80) = new PrimitiveType(OPTIONAL, BINARY, "in_dst_mac")

  // 81-90
  netflowFields(81) = new PrimitiveType(OPTIONAL, BINARY, "out_src_mac")
  netflowFields(82) = new PrimitiveType(OPTIONAL, BINARY, "if_name")
  netflowFields(83) = new PrimitiveType(OPTIONAL, BINARY, "if_desc")
  netflowFields(84) = new PrimitiveType(OPTIONAL, BINARY, "sampler_name")
  netflowFields(85) = new PrimitiveType(OPTIONAL, INT64, "in_permanent_bytes")
  netflowFields(86) = new PrimitiveType(OPTIONAL, INT64, "in_permanent_pkts")
  netflowFields(87) = null // *Vendor Proprietary
  netflowFields(88) = new PrimitiveType(OPTIONAL, INT32, "fragment_offset")
  netflowFields(89) = new PrimitiveType(OPTIONAL, BINARY, "forwarding_status")
  netflowFields(90) = new PrimitiveType(OPTIONAL, BINARY, "mpls_pal_rd")

  // 91-100
  netflowFields(91) = new PrimitiveType(OPTIONAL, INT32, "mpls_prefix_len")
  netflowFields(92) = new PrimitiveType(OPTIONAL, INT64, "src_traffic_index")
  netflowFields(93) = new PrimitiveType(OPTIONAL, INT64, "dst_traffic_index")
  netflowFields(94) = new PrimitiveType(OPTIONAL, BINARY, "application_description")
  netflowFields(95) = new PrimitiveType(OPTIONAL, BINARY, "application_tag")
  netflowFields(96) = new PrimitiveType(OPTIONAL, BINARY, "application_name")
  netflowFields(97) = null // *Vendor Proprietary
  netflowFields(98) = new PrimitiveType(OPTIONAL, INT32, "postipdiffservcodepoint")
  netflowFields(99) = new PrimitiveType(OPTIONAL, INT64, "rireplication_factor")
  netflowFields(100) = new PrimitiveType(OPTIONAL, BINARY, "DEPRECATED")

  object FieldType extends Enumeration {
    type FieldType = Value

    val NETFLOW, BGP, HEADER = Value
  }

  val validHeader = headerFields.filter(_ != null)
  val validNetflow = netflowFields.filter(_ != null)
  val validBgp = bgpFields.filter(_ != null)

  val overallSchema = new MessageType("OverallData", validHeader ++ validNetflow ++ validBgp: _*)

  def getPosAndType(ft: FieldType.Value, selfIndex: Int): (Int, Type) = {

    def checkSelfNull(types: Array[Type]) = {
      if (types(selfIndex) == null) throw new NetFlowException("Getting a invalid field")
    }

    def getNotNulls(types: Array[Type]): Int = {
      var num = 0
      for (i <- 0 until selfIndex) {
        if (types(i) != null) {
          num += 1
        }
      }
      num
    }

    try {
      ft match {
        case FieldType.HEADER =>
          checkSelfNull(headerFields)
          (getNotNulls(headerFields), headerFields(selfIndex))

        case FieldType.NETFLOW =>
          checkSelfNull(netflowFields)
          (validHeader.length + getNotNulls(netflowFields), netflowFields(selfIndex))

        case FieldType.BGP =>
          checkSelfNull(bgpFields)
          (validHeader.length + validNetflow.length + getNotNulls(bgpFields), bgpFields(selfIndex))
      }

    } catch {
      case e: NetFlowException =>
        logInfo(e.getMessage)
        (-1, null)
    }
  }
}
