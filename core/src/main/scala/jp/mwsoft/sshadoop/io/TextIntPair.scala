package jp.mwsoft.sshadoop.io

import org.apache.hadoop.io._

class TextIntPair extends PairWritable[Text, IntWritable](new Text(), new IntWritable()) {

  def this(value1: String, value2: Int) {
    this()
    first.set(value1)
    second.set(value2)
  }

}