/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.mwsoft.sshadoop.mapreduce

import org.apache.hadoop.mapreduce.{ Mapper }
import jp.mwsoft.sshadoop.util.ImplicitConversions
import org.apache.hadoop.util.ReflectionUtils

abstract class SMapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT](
  implicit keyOutType: Manifest[KEY_OUT], valOutType: Manifest[VAL_OUT])
    extends SMapperBase[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT] {

  def outputKeyClass = keyOutType.erasure.asInstanceOf[Class[KEY_OUT]]
  def outputValueClass = valOutType.erasure.asInstanceOf[Class[VAL_OUT]]

  val outKey = ReflectionUtils.newInstance(outputKeyClass, null)
  val outValue = ReflectionUtils.newInstance(outputValueClass, null)
}

trait SMapperBase[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT]
    extends Mapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT] with ImplicitConversions {

  type Context = Mapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT]#Context

}
