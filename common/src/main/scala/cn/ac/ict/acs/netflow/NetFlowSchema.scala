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
package cn.ac.ict.acs.netflow

import org.apache.spark.sql.types._

@deprecated
object NetFlowSchema extends java.io.Serializable {
  private val NULLABLE: Boolean = true

  val tableSchema =
    StructType(
      StructField("flow_time", LongType, NULLABLE) ::
        StructField("in_bytes", BinaryType, NULLABLE) ::
        StructField("in_pkts", BinaryType, NULLABLE) ::
        StructField("flows", BinaryType, NULLABLE) ::
        StructField("protocol", ShortType, NULLABLE) ::
        StructField("src_tos", ShortType, NULLABLE) ::
        StructField("tcp_flags", ShortType, NULLABLE) ::
        StructField("l4_src_port", IntegerType, NULLABLE) ::
        StructField("ipv4_src_addr", BinaryType, NULLABLE) ::
        StructField("src_mask", ShortType, NULLABLE) ::
        StructField("input_snmp", BinaryType, NULLABLE) ::

        StructField("l4_dst_port", IntegerType, NULLABLE) ::
        StructField("ipv4_dst_addr", BinaryType, NULLABLE) ::
        StructField("dst_mask", ShortType, NULLABLE) ::
        StructField("output_snmp", BinaryType, NULLABLE) ::
        StructField("ipv4_next_hop", BinaryType, NULLABLE) ::
        StructField("src_as", BinaryType, NULLABLE) ::
        StructField("dst_as", BinaryType, NULLABLE) ::
        StructField("bgp_ipv4_next_hop", BinaryType, NULLABLE) ::
        StructField("mul_dst_pkts", BinaryType, NULLABLE) ::
        StructField("mul_dst_bytes", BinaryType, NULLABLE) ::

        StructField("last_switched", LongType, NULLABLE) ::
        StructField("first_switched", LongType, NULLABLE) ::
        StructField("out_bytes", BinaryType, NULLABLE) ::
        StructField("out_pkts", BinaryType, NULLABLE) ::
        StructField("min_pkt_lngth", IntegerType, NULLABLE) ::
        StructField("max_pkt_lngth", IntegerType, NULLABLE) ::
        StructField("ipv6_src_addr", BinaryType, NULLABLE) ::
        StructField("ipv6_dst_addr", BinaryType, NULLABLE) ::
        StructField("ipv6_src_mask", ShortType, NULLABLE) ::
        StructField("ipv6_dst_mask", ShortType, NULLABLE) ::

        StructField("ipv6_flow_label", BinaryType, NULLABLE) ::
        StructField("icmp_type", IntegerType, NULLABLE) ::
        StructField("mul_igmp_type", ShortType, NULLABLE) ::
        StructField("sampling_interval", IntegerType, NULLABLE) ::
        StructField("sampling_algorithm", ShortType, NULLABLE) ::
        StructField("flow_active_timeout", IntegerType, NULLABLE) ::
        StructField("flow_inactive_timeout", IntegerType, NULLABLE) ::
        StructField("engine_type", ShortType, NULLABLE) ::
        StructField("engine_id", ShortType, NULLABLE) ::
        StructField("total_bytes_exp", BinaryType, NULLABLE) ::

        StructField("total_pkts_exp", BinaryType, NULLABLE) ::
        StructField("total_flows_exp", BinaryType, NULLABLE) ::
        StructField("null1", ShortType, NULLABLE) ::
        StructField("ipv4_src_prefix", IntegerType, NULLABLE) ::
        StructField("ipv4_dst_prefix", IntegerType, NULLABLE) ::
        StructField("mpls_top_label_type", ShortType, NULLABLE) ::
        StructField("mpls_top_label_ip_addr", BinaryType, NULLABLE) ::
        StructField("flow_sampler_id", ShortType, NULLABLE) ::
        StructField("flow_sampler_mode", ShortType, NULLABLE) ::
        StructField("flow_sampler_random_interval", LongType, NULLABLE) ::

        StructField("null2", ShortType, NULLABLE) ::
        StructField("min_ttl", ShortType, NULLABLE) ::
        StructField("max_ttl", ShortType, NULLABLE) ::
        StructField("ipv4_ident", IntegerType, NULLABLE) ::
        StructField("dst_tos", ShortType, NULLABLE) ::
        StructField("in_src_mac", BinaryType, NULLABLE) ::
        StructField("out_dst_mac", BinaryType, NULLABLE) ::
        StructField("src_vlan", IntegerType, NULLABLE) ::
        StructField("dst_vlan", IntegerType, NULLABLE) ::
        StructField("ip_protocol_version", ShortType, NULLABLE) ::

        StructField("direction", ShortType, NULLABLE) ::
        StructField("ipv6_next_hop", BinaryType, NULLABLE) ::
        StructField("bpg_ipv6_next_hop", BinaryType, NULLABLE) ::
        StructField("ipv6_option_headers", IntegerType, NULLABLE) ::
        StructField("null3", ShortType, NULLABLE) ::
        StructField("null4", ShortType, NULLABLE) ::
        StructField("null5", ShortType, NULLABLE) ::
        StructField("null6", ShortType, NULLABLE) ::
        StructField("null7", ShortType, NULLABLE) ::
        StructField("mpls_label_1", BinaryType, NULLABLE) ::

        StructField("mpls_label_2", BinaryType, NULLABLE) ::
        StructField("mpls_label_3", BinaryType, NULLABLE) ::
        StructField("mpls_label_4", BinaryType, NULLABLE) ::
        StructField("mpls_label_5", BinaryType, NULLABLE) ::
        StructField("mpls_label_6", BinaryType, NULLABLE) ::
        StructField("mpls_label_7", BinaryType, NULLABLE) ::
        StructField("mpls_label_8", BinaryType, NULLABLE) ::
        StructField("mpls_label_9", BinaryType, NULLABLE) ::
        StructField("mpls_label_10", BinaryType, NULLABLE) ::
        StructField("in_dst_mac", BinaryType, NULLABLE) ::

        StructField("out_src_mac", BinaryType, NULLABLE) ::
        StructField("if_name", BinaryType, NULLABLE) ::
        StructField("if_desc", BinaryType, NULLABLE) ::
        StructField("sampler_name", BinaryType, NULLABLE) ::
        StructField("in_permanent_bytes", BinaryType, NULLABLE) ::
        StructField("in_permanent_pkts", BinaryType, NULLABLE) ::
        StructField("null8", ShortType, NULLABLE) ::
        StructField("fragment_offset", IntegerType, NULLABLE) ::
        StructField("forwarding_status", ShortType, NULLABLE) ::
        StructField("mpls_pal_rd", BinaryType, NULLABLE) ::

        StructField("mpls_prefix_len", ShortType, NULLABLE) ::
        StructField("src_traffic_index", LongType, NULLABLE) ::
        StructField("dst_traffic_index", LongType, NULLABLE) ::
        StructField("application_description", BinaryType, NULLABLE) ::
        StructField("application_tag", BinaryType, NULLABLE) ::
        StructField("application_name", BinaryType, NULLABLE) ::
        StructField("null9", ShortType, NULLABLE) ::
        StructField("postipdiffservcodepoint", ShortType, NULLABLE) ::
        StructField("rireplication_factor", IntegerType, NULLABLE) ::
        StructField("deprecated", BinaryType, NULLABLE) :: Nil)
}
