/*
 * Copyright (c) 2012 Watanabe Masato
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package jp.mwsoft.sshadoop.mapreduce

import org.apache.hadoop.io.{ BooleanWritable, ByteWritable, IntWritable, LongWritable, FloatWritable, DoubleWritable }
import org.apache.hadoop.io.{ Text, BytesWritable, ArrayWritable }

import org.apache.hadoop.io.MapWritable

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
}