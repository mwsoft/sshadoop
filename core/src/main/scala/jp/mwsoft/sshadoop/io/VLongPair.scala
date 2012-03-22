package jp.mwsoft.sshadoop.io

import org.apache.hadoop.io._

class VLongPair extends PairWritable[VLongWritable, VLongWritable](new VLongWritable(), new VLongWritable()) {
  def this(value1: Long, value2: Long) {
    this()
    first.set(value1)
    second.set(value2)
  }
}