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
package cn.ac.ict.acs.netflow.query.master

import java.util.Map.Entry

/**
 * A convenience class to define a Least-Recently-Used Cache with a maximum size.
 * The oldest entries by time of last access will be removed when the number of entries exceeds
 * cacheSize.
 * For definitions of cacheSize and loadingFactor, see the docs for java.util.LinkedHashMap
 * @see LinkedHashMap
 */
class LRUCache[K, V](cacheSize: Int, loadingFactor: Float  = 0.75F) {

  private val cache = {
    val initialCapacity = math.ceil(cacheSize / loadingFactor).toInt + 1
    new java.util.LinkedHashMap[K, V](initialCapacity, loadingFactor, true) {
      protected override def removeEldestEntry(p1: Entry[K, V]): Boolean = size() > cacheSize
    }
  }

  private var cacheMiss = 0
  private var cacheHit = 0

  /** size of the cache. This is an exact number and runs in constant time */
  def size: Int = cache.size()

  /** @return TRUE if the cache contains the key */
  def containsKey(k: K): Boolean = cache.get(k) != null

  /** @return the value in cache or load a new value into cache */
  def get(k: K, v: => V): V = {
    cache.get(k) match {
      case null =>
        val evaluatedV = v
        cache.put(k, evaluatedV)
        cacheMiss += 1
        evaluatedV
      case vv =>
        cacheHit += 1
        vv
    }
  }

  def cacheHitRatio: Double = cacheMiss.toDouble / math.max(cacheMiss + cacheHit, 1)

  def put(k: K, v: V): V = cache.put(k, v)

  def get(k: K): Option[V] = Option(cache.get(k))
}

