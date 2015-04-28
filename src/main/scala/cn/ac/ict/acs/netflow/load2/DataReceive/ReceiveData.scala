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
package cn.ac.ict.acs.netflow.load2.DataReceive

import java.nio.ByteBuffer

import akka.actor.Actor
import akka.actor.Actor.Receive

/**
 * Created by ayscb on 2015/4/21.
 */

sealed trait Message

case class receiveFromUDP( data :ByteBuffer ) extends Message
case class receiveFromFile ( fileStr : String ) extends Message
case class receiveFromArray ( data : Array[Byte] ,offset : Int, length :Int ) extends Message
case class receiveFromBuffer ( data : ByteBuffer ) extends Message


class workerActor extends Actor{
  override def receive: Receive = ???
}
