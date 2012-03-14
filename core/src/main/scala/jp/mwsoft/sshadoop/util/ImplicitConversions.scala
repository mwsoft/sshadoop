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

import org.apache.hadoop.io.{ BooleanWritable, ByteWritable, IntWritable, LongWritable, FloatWritable, DoubleWritable }
import org.apache.hadoop.io.{ Text, BytesWritable, ArrayWritable }
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.mapreduce.MapContext

object ImplicitConversions extends ImplicitConversions

/**
 * implicit conversions
 * ex: Int <=> IntWritable, String <=> Text
 *
 * @author Watanabe Masato
 */
trait ImplicitConversions {

  val clsBooleanWritable = classOf[BooleanWritable]
  val clsByteWritable = classOf[ByteWritable]
  val clsIntWritable = classOf[IntWritable]
  val clsLongWritable = classOf[LongWritable]
  val clsFloatWritable = classOf[FloatWritable]
  val clsDoubleWritable = classOf[DoubleWritable]
  val clsText = classOf[Text]
  val clsBytesWritable = classOf[BytesWritable]
  val clsArrayWritable = classOf[ArrayWritable]

  implicit def hadoopBooleanWritable2boolean(value: BooleanWritable) = value.get
  implicit def boolean2hadoopBooleanWritable(value: Boolean) = new BooleanWritable(value)

  implicit def hadoopByteWritable2byte(value: ByteWritable) = value.get
  implicit def byte2hadoopByteWritable(value: Byte) = new ByteWritable(value)

  implicit def hadoopIntWritable2int(value: IntWritable) = value.get
  implicit def int2hadoopIntWritable(value: Int) = new IntWritable(value)

  implicit def hadoopLongWritable2long(value: LongWritable) = value.get
  implicit def long2hadoopLongWritable(value: Long) = new LongWritable(value)

  implicit def hadoopFloatWritable2float(value: FloatWritable) = value.get
  implicit def float2hadoopFloatWritable(value: Float) = new FloatWritable(value)

  implicit def hadoopDoubleWritable2double(value: DoubleWritable) = value.get
  implicit def double2hadoopDoubleWritable(value: Double) = new DoubleWritable(value)

  implicit def hadoopText2string(value: Text) = value.toString
  implicit def string2hadoopText(value: String) = new Text(value)

  implicit def hadoopBytesWritable2bytes(value: BytesWritable) = value.get
  implicit def bytes2hadoopBytesWritable(value: Array[Byte]) = new BytesWritable(value)

  implicit def javaIterator2scalaIterator[A](value: java.util.Iterator[A]) = new Iterator[A] {
    def hasNext = value.hasNext
    def next = value.next
  }

  implicit def hadoopContext2scalaIterator[A, B](context: MapContext[A, B, _, _]) = new Iterator[(A, B)] {
    private var hasnext = context.nextKeyValue()
    def hasNext = hasnext
    def next = {
      if (hasnext) {
        val result = (context.getCurrentKey(), context.getCurrentValue())
        hasnext = context.nextKeyValue()
        result
      } else Iterator.empty.next
    }
  }
}