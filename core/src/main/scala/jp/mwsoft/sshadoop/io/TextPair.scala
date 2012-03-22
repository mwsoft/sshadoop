package jp.mwsoft.sshadoop.io

import org.apache.hadoop.io._

class TextPair extends PairWritable[Text, Text](new Text(), new Text()) {
  def this(value1: String, value2: String) {
    this()
    first.set(value1)
    second.set(value2)
  }
}
