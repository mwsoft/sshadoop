package jp.mwsoft.sshadoop.io

import org.apache.hadoop.io._

class VIntPair extends PairWritable[VIntWritable, VIntWritable](new VIntWritable(), new VIntWritable()) {
  def this(value1: Int, value2: Int) {
    this()
    first.set(value1)
    second.set(value2)
  }
}