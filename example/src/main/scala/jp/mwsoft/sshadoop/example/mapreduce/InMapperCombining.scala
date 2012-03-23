package jp.mwsoft.sshadoop.example.mapreduce

import jp.mwsoft.sshadoop.util.STool
import jp.mwsoft.sshadoop.mapreduce._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer

object InMapperCombining extends STool {

  def runJob(args: Array[String]): Boolean = {

    new SJob(getConf(), "wordCount1")
      .mapper(new MyMapper1())
      .reducerClass(classOf[LongSumReducer[_]], classOf[Text], classOf[LongWritable])
      .fileInputPaths("data/in")
      .fileOutputPath("data/out/1")
      .waitForCompletion(true)

    new SJob(getConf(), "wordCount2")
      .mapper(new MyMapper2())
      .reducerClass(classOf[LongSumReducer[_]], classOf[Text], classOf[LongWritable])
      .fileInputPaths("data/in")
      .fileOutputPath("data/out/2")
      .waitForCompletion(true)
  }

  class MyMapper1 extends SMapper[LongWritable, Text, Text, LongWritable] {

    var countMap: collection.mutable.HashMap[String, Long] = null

    override def setup(context: Context) {
      countMap = new collection.mutable.HashMap[String, Long]()
    }

    override def map(key: LongWritable, value: Text, context: Context) {
      for (word <- value.toString.split("\\s"))
        countMap += word -> (countMap.getOrElse(word, 0L) + 1L)
    }

    override def cleanup(context: Context) {
      for ((key, value) <- countMap)
        context.write(key, value)
      countMap.clear()
    }
  }

  class MyMapper2 extends SMapper[LongWritable, Text, Text, LongWritable] {

    override def run(context: Context) {

      // in-mapper combining
      var map = new collection.mutable.HashMap[String, Long]()
      while (context.nextKeyValue()) {
        for (word <- context.getCurrentValue().toString.split("\\s"))
          map += word -> (map.getOrElse(word, 0L) + 1L)
      }

      // write
      for ((key, value) <- map)
        context.write(key, value)
    }

  }

  class MyMapper3 extends SMapper[LongWritable, Text, Text, LongWritable] {

    override def run(context: Context) {

      // in-mapper combining
      var map = new collection.mutable.HashMap[String, Long]()
      for ((key, value) <- context) {
        for (word <- value.toString.split("\\s"))
          map += word -> (map.getOrElse(word, 0L) + 1L)
      }

      // write
      for ((key, value) <- map)
        context.write(key, value)
    }

  }

}