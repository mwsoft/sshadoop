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
package jp.mwsoft.sshadoop.util

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{ Tool, ToolRunner }
import jp.mwsoft.sshadoop.mapreduce.ImplicitConversions

trait STool extends Configured with Tool with ImplicitConversions {

  def main(args: Array[String]) {
    exit(ToolRunner.run(this, args))
  }

  override def run(args: Array[String]): Int = if (runJob(args)) 0 else 1

  def runJob(args: Array[String]): Boolean

}