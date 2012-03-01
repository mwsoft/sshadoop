package jp.mwsoft.sshadoop.mapreduce.example

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.{ Configured, Configuration }
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Mapper
import org.atilika.kuromoji.Tokenizer
import jp.mwsoft.sshadoop.mapreduce._

object WordCountSample extends Configured with Tool with App {

  run(args)

  override def run(args: Array[String]): Int = {

    val job = new SJob(getConf(), "jobName")

    job mapper (new MyMapper()) reducer (new MyReducer())

    if (job.waitForCompletion(true)) 0 else 1
  }

  class MyMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
    override def map(key: LongWritable, value: Text, context: Context) {
      for (str <- value.split("\t")) context.write(str, 1)
    }
  }

  class MyReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce(key: Text, values: Iterator[LongWritable], context: Context) {
      context.write(key, values.map(_.get).sum)
    }
  }

  class MyCombiner extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce(key: Text, values: Iterator[LongWritable], context: Context) {
      context.write(key, values.map(_.get).sum)
    }
  }
}
