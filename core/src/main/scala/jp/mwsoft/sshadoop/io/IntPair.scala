package jp.mwsoft.sshadoop.io

import org.apache.hadoop.io._

class IntPair extends PairWritable[IntWritable, IntWritable](new IntWritable(), new IntWritable()) {
  def this(value1: Int, value2: Int) {
    this()
    first.set(value1)
    second.set(value2)
  }
}