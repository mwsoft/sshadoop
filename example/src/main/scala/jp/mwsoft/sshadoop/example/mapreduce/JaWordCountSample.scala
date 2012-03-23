package jp.mwsoft.sshadoop.example.mapreduce

import scala.compat.Platform.currentTime
import org.apache.hadoop.io.{ Text, LongWritable }
import org.atilika.kuromoji.Tokenizer
import jp.mwsoft.sshadoop.mapreduce._
import jp.mwsoft.sshadoop.util.STool
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.io.BytesWritable

/**
 * 日本語用ワードカウント。Kuromoji利用
 * http://www.atilika.org/
 */
object JaWordCountSample extends STool {

  def runJob(args: Array[String]): Boolean = {
    new SJob(getConf(), "jobName")
      .mapper(new JaWordCountMapper())
      .reducer(new JaWordCountReducer())
      .combiner(new JaWordCountCombiner())
      .inputFormatClass(classOf[SequenceFileInputFormat[BytesWritable, Text]])
      .fileInputPaths("/user/hive/warehouse/uni_tweets/dt=20120201")
      .fileOutputPath("data/out/" + currentTime)
      .waitForCompletion(true)
  }

  class JaWordCountMapper extends SMapper[BytesWritable, Text, Text, LongWritable] {
    val tokenizer = Tokenizer.builder().userDictionary(getClass.getResourceAsStream("user.dic")).build()
    override def map(key: BytesWritable, value: Text, context: Context) {
      tokenizer.tokenize(value).iterator() foreach (x => context.write(x.getSurfaceForm(), 1L))
    }
  }

  class JaWordCountReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce(key: Text, values: Iterator[LongWritable], context: Context) {
      val sum = (0L /: values) { _ + _ }
      if (sum >= 10) context.write(key, sum)
    }
  }

  class JaWordCountCombiner extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce(key: Text, values: Iterator[LongWritable], context: Context) {
      val sum = (0L /: values) { _ + _ }
      if (sum >= 2) context.write(key, sum)
    }
  }
}

