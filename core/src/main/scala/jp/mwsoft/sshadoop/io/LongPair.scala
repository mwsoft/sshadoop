package jp.mwsoft.sshadoop.io

import org.apache.hadoop.io._

class LongPair extends PairWritable[LongWritable, LongWritable](new LongWritable(), new LongWritable()) {
  def this(value1: Long, value2: Long) {
    this()
    first.set(value1)
    second.set(value2)
  }
}