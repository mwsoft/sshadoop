package jp.mwsoft.sshadoop.io

import org.apache.hadoop.io._

class TextVLongPair extends PairWritable[Text, VLongWritable](new Text(), new VLongWritable()) {

  def this(value1: String, value2: Long) {
    this()
    first.set(value1)
    second.set(value2)
  }

}