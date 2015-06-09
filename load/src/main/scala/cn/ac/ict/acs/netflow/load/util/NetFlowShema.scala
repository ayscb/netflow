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
package cn.ac.ict.acs.netflow.load.util

import parquet.schema.PrimitiveType.PrimitiveTypeName._
import parquet.schema.Type.Repetition.{ OPTIONAL, REPEATED, REQUIRED }
import parquet.schema.{ GroupType, MessageType, MessageTypeParser, PrimitiveType }

import cn.ac.ict.acs.netflow.NetFlowConf

/**
 * netflow v9 version for parquet
 * Created by ayscb on 2015/4/14.
 */
object NetFlowShema {

  /* single base schema */
  val singleSchema = new MessageType("netflow",
    new PrimitiveType(REQUIRED, INT64, "unixSeconds"),

    // 1-10
    new PrimitiveType(OPTIONAL, INT64, "in_bytes"),
    new PrimitiveType(OPTIONAL, INT64, "in_pkts"),
    new PrimitiveType(OPTIONAL, INT64, "flows"),
    new PrimitiveType(OPTIONAL, INT32, "protocol"),
    new PrimitiveType(OPTIONAL, INT32, "src_tos"),
    new PrimitiveType(OPTIONAL, INT32, "tcp_flags"),
    new PrimitiveType(OPTIONAL, INT32, "l4_src_port"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 4, "ipv4_src_addr"),
    new PrimitiveType(OPTIONAL, INT32, "src_mask"),
    new PrimitiveType(OPTIONAL, INT64, "input_snmp"),

    // 11-20
    new PrimitiveType(OPTIONAL, INT32, "l4_dst_port"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 4, "ipv4_dst_addr"),
    new PrimitiveType(OPTIONAL, INT32, "dst_mask"),
    new PrimitiveType(OPTIONAL, INT64, "output_snmp"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 4, "ipv4_next_hop"),
    new PrimitiveType(OPTIONAL, INT64, "src_as"),
    new PrimitiveType(OPTIONAL, INT64, "dst_as"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 4, "bgp_ipv4_next_hop"),
    new PrimitiveType(OPTIONAL, INT64, "mul_dst_pkts"),
    new PrimitiveType(OPTIONAL, INT64, "mul_dst_bytes"),

    // 21-30
    new PrimitiveType(OPTIONAL, INT64, "last_switched"),
    new PrimitiveType(OPTIONAL, INT64, "first_switched"),
    new PrimitiveType(OPTIONAL, INT64, "out_bytes"),
    new PrimitiveType(OPTIONAL, INT64, "out_pkts"),
    new PrimitiveType(OPTIONAL, INT32, "min_pkt_lngth"),
    new PrimitiveType(OPTIONAL, INT32, "max_pkt_lngth"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 16, "ipv6_src_addr"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 16, "ipv6_dst_addr"),
    new PrimitiveType(OPTIONAL, INT32, "ipv6_src_mask"),
    new PrimitiveType(OPTIONAL, INT32, "ipv6_dst_mask"),

    // 31-40
    new PrimitiveType(OPTIONAL, INT32, "ipv6_flow_label"), // in ipv6 only use 20bit
    new PrimitiveType(OPTIONAL, INT32, "icmp_type"),
    new PrimitiveType(OPTIONAL, INT32, "mul_igmp_type"),
    new PrimitiveType(OPTIONAL, INT64, "sampling_interval"),
    new PrimitiveType(OPTIONAL, INT32, "sampling_algorithm"),
    new PrimitiveType(OPTIONAL, INT32, "flow_active_timeout"),
    new PrimitiveType(OPTIONAL, INT32, "flow_inactive_timeout"),
    new PrimitiveType(OPTIONAL, INT32, "engine_type"),
    new PrimitiveType(OPTIONAL, INT32, "engine_id"),
    new PrimitiveType(OPTIONAL, INT64, "total_bytes_exp"),

    // 41-50
    new PrimitiveType(OPTIONAL, INT64, "total_pkts_exp"),
    new PrimitiveType(OPTIONAL, INT64, "total_flows_exp"),
    // *Vendor Proprietary
    new PrimitiveType(OPTIONAL, INT64, "ipv4_src_prefix"),
    new PrimitiveType(OPTIONAL, INT64, "ipv4_dst_prefix"),
    new PrimitiveType(OPTIONAL, INT32, "mpls_top_label_type"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 4, "mpls_top_label_ip_addr"), // ????
    new PrimitiveType(OPTIONAL, INT32, "flow_sampler_id"),
    new PrimitiveType(OPTIONAL, INT32, "flow_sampler_mode"),
    new PrimitiveType(OPTIONAL, INT64, "flow_sampler_random_interval"),

    // 51-60
    // *Vendor Proprietary
    new PrimitiveType(OPTIONAL, INT32, "min_ttl"),
    new PrimitiveType(OPTIONAL, INT32, "max_ttl"),
    new PrimitiveType(OPTIONAL, INT32, "ipv4_ident"),
    new PrimitiveType(OPTIONAL, INT32, "dst_tos"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 6, "in_src_mac"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 6, "out_dst_mac"),
    new PrimitiveType(OPTIONAL, INT32, "src_vlan"),
    new PrimitiveType(OPTIONAL, INT32, "dst_vlan"),
    new PrimitiveType(OPTIONAL, INT32, "ip_protocol_version"),

    // 61-70
    new PrimitiveType(OPTIONAL, INT32, "direction"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 16, "ipv6_next_hop"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 16, "bpg_ipv6_next_hop"),
    new PrimitiveType(OPTIONAL, INT64, "ipv6_option_headers"),
    // *Vendor Proprietary
    // *Vendor Proprietary
    // *Vendor Proprietary
    // *Vendor Proprietary
    // *Vendor Proprietary
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_1"), // contain 20bit

    // 71-80
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_2"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_3"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_4"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_5"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_6"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_7"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_8"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_9"), // contain 20bit
    new PrimitiveType(OPTIONAL, INT32, "mpls_label_10"), // contain 20bit
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 6, "in_dst_mac"),

    // 81-90
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 6, "out_src_mac"),
    new PrimitiveType(OPTIONAL, BINARY, "if_name"),
    new PrimitiveType(OPTIONAL, BINARY, "if_desc"),
    new PrimitiveType(OPTIONAL, BINARY, "sampler_name"),
    new PrimitiveType(OPTIONAL, INT64, "in_permanent_bytes"),
    new PrimitiveType(OPTIONAL, INT64, "in_permanent_pkts"),
    // *Vendor Proprietary
    new PrimitiveType(OPTIONAL, INT32, "fragment_offset"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 1, "forwarding_status"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 8, "mpls_pal_rd"),

    // 91-100
    new PrimitiveType(OPTIONAL, INT32, "mpls_prefix_len"),
    new PrimitiveType(OPTIONAL, INT64, "src_traffic_index"),
    new PrimitiveType(OPTIONAL, INT64, "dst_traffic_index"),
    new PrimitiveType(OPTIONAL, BINARY, "application_description"),
    new PrimitiveType(OPTIONAL, BINARY, "application_tag"),
    new PrimitiveType(OPTIONAL, BINARY, "application_name"),
    // *Vendor Proprietary
    new PrimitiveType(OPTIONAL, INT32, "postipdiffservcodepoint"),
    new PrimitiveType(OPTIONAL, INT64, "rireplication_factor"),
    new PrimitiveType(OPTIONAL, BINARY, "DEPRECATED"))


  def mapKey2Clm(key: Int): Int = idxToColumn(key)

  // since there are some null column ,
  // we skip all the null column
  private val idxToColumn = Array(0,
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, -1, 42, 43, 44, 45, 46, 47, 48,
    -1, 49, 50, 51, 52, 53, 54, 55, 56, 57,
    58, 59, 60, 61, -1, -1, -1, -1, -1, 62,
    63, 64, 65, 66, 67, 68, 69, 70, 71, 72,
    73, 74, 75, 76, 77, 78, -1, 79, 80, 81,
    82, 83, 84, 85, 86, 87, 88, 89, 90, 91)
}
