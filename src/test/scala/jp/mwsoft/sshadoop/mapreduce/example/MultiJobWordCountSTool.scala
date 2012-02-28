package jp.mwsoft.sshadoop.mapreduce.example

import scala.compat.Platform.currentTime
import org.apache.hadoop.util.{ Tool, ToolRunner }
import org.apache.hadoop.conf.{ Configured, Configuration }
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapreduce.{ Job, Mapper }
import org.atilika.kuromoji.Tokenizer
import jp.mwsoft.sshadoop.mapreduce.{ SMapper, SReducer, STool, SJob }

object MultiJobWordCountWithSTool extends App {
  val ret = ToolRunner.run(WordCount, args)
  exit(if (ret == 0) ToolRunner.run(WordCountSort, args) else ret)

  /** 日本語用ワードカウント */
  object WordCount extends STool {
    val tokenizer = Tokenizer.builder().build()
    val output = "data/out_" + currentTime

    override def runJob(args: Array[String]): Boolean = {
      new SJob(getConf(), "jobName").
        mapper(new MyMapper()).reducer(new MyReducer()).
        fileInputPath("data/in").fileOutputPath(output).waitForCompletion(true)
    }

    class MyMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
      override def map(key: LongWritable, value: Text, context: Context) {
        tokenizer.tokenize(value).iterator() foreach (x => context.write(x.getSurfaceForm(), 1L))
      }
    }

    class MyReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
      override def reduce(key: Text, values: Iterator[LongWritable], context: Context) {
        context.write(key, values.map(_.get).sum)
      }
    }

  }

  /** ワードカウント結果を数値順でソート */
  object WordCountSort extends STool {

    override def runJob(args: Array[String]): Boolean = {
      new SJob(getConf(), "jobName").
        mapper(new MyMapper()).reducer(new MyReducer()).
        fileInputPath(WordCount.output).fileOutputPath("data/out_" + currentTime).waitForCompletion(true)
    }

    class MyMapper extends SMapper[LongWritable, Text, LongWritable, Text] {
      override def map(key: LongWritable, value: Text, context: Context) {
        val items = value.split("\t")
        if (items.size == 2) context.write(items(1).toLong, items(0))
      }
    }

    class MyReducer extends SReducer[LongWritable, Text, Text, LongWritable] {
      override def reduce(key: LongWritable, values: Iterator[Text], context: Context) {
        values foreach (x => context.write(x, key))
      }
    }
  }
}

