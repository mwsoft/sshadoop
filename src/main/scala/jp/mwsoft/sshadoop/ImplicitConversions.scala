package jp.mwsoft.sshadoop

import org.apache.hadoop.io.{ BooleanWritable, ByteWritable, IntWritable, LongWritable, FloatWritable, DoubleWritable }
import org.apache.hadoop.io.{ Text, BytesWritable, ArrayWritable }
import org.apache.hadoop.io.MapWritable

object ImplicitConversions extends ImplicitConversions

trait ImplicitConversions {

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