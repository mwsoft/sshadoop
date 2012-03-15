package jp.mwsoft.sshadoop.mapreduce.example

import org.apache.hadoop.io._
import jp.mwsoft.sshadoop.io._
import jp.mwsoft.sshadoop.mapreduce._
import jp.mwsoft.sshadoop.util.STool

object Pair extends STool {

  def runJob(args: Array[String]): Boolean = {
    new SJob(getConf(), "jobName")
      //.mapper(new PairMapper())
      .reducer(new PairReducer())
      .combiner(new PairCombiner())
      .fileInputPaths("data/in")
      .fileOutputPath("data/out/6")
      .waitForCompletion(true)
  }

  class PairMapper extends SMapper[LongWritable, Text, Text, PairWritable[_, _]] {
    override def map(key: LongWritable, value: Text, context: Context) {
      for (word <- value.split("\\s")) {
        val pw = new PairWritable(new Text(word), new LongWritable(1L))
        context.write(word, pw)
      }
    }
  }

  def reduceMap(values: Iterator[MapWritable]): java.util.Map[Text, LongWritable] = {
    val map = new java.util.HashMap[Text, LongWritable]()
    for (value <- values; entry <- value.entrySet.iterator) {
      val key = entry.getKey.asInstanceOf[Text]
      val value = entry.getValue.asInstanceOf[LongWritable].get
      println(Option(map.get(key)).getOrElse(0L))
      map.put(key, Option(map.get(key)).getOrElse(0L))
    }
    map
  }

  class PairCombiner extends SReducer[Text, MapWritable, Text, MapWritable] {
    override def reduce(key: Text, values: Iterator[MapWritable], context: Context) {
      val map = reduceMap(values)
      val mapWritable = new MapWritable()
      mapWritable.putAll(map)
      context.write(key, mapWritable)
    }
  }

  class PairReducer extends SReducer[Text, MapWritable, Text, LongWritable] {
    override def reduce(key: Text, values: Iterator[MapWritable], context: Context) {
      val map = reduceMap(values)
      for (entry <- map.entrySet.iterator)
        context.write(key.toString + "," + entry.getKey, entry.getValue)
    }
  }

}