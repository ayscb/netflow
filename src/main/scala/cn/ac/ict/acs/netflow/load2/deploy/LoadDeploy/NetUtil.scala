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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load2.deploy.LoadDeploy

import java.io.IOException
import java.net.{Socket, InetAddress}

import scala.util.Random

/**
 * Created by ayscb on 2015/5/5.
 */
object NetUtil {

  private val random = new Random()

  def getAvailablePort : Int = {
    // service port form 1024 to 65535  , so 64511 = 65535-1024
    var port = Math.abs(random.nextInt(64511)) + 1024
    while( isLocalProtUsing(port) ) port = Math.abs(random.nextInt(64511)) + 1024
    port
  }

  private def isLocalProtUsing ( port : Int ) : Boolean = {
    isProtUsing("127.0.0.1", port)
  }

  private def isProtUsing(  host : String, port : Int ) : Boolean = {
    val address = InetAddress.getByName(host)
    try {
      new Socket(address, port)   // if can connect with the port , the port is using
      true
    }
    catch {
      case x:IOException => false
    }
  }
}
