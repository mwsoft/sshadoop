package jp.mwsoft.sshadoop.mapreduce.example

import jp.mwsoft.sshadoop.mapreduce._
import org.apache.hadoop.io.{ Text, LongWritable }
import java.util.regex.Pattern
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

object Grep extends STool {

  val PATTERN = "mapreduce.mapper.regex"
  val GROUP = "mapreduce.mapper.regexmapper..group"

  def runJob(args: Array[String]): Boolean = {
    if (args.size < 3)
      throw new IllegalArgumentException("Usage : grep input output pattern")

    val conf = getConf()
    conf.set(PATTERN, args(2))
    if (args.size > 3) conf.setInt(GROUP, args(3).trim.toInt)

    // run grep job
    val grepResult = new SJob(conf, "grep-search")
      .mapper(new GrepMapper())
      .combiner(new LongSumReducer())
      .reducer(new LongSumReducer())
      .fileInputPath(args(0))
      .outputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
      .waitForCompletion(true)

    // run sort job
    if (grepResult)
      new SJob(conf, "grep-sort")
        .mapper(new InverseMapper())
        .numReduceTasks(1)
        .fileOutputPath(args(1))
        .waitForCompletion(true)
    else false
  }

  class GrepMapper extends SMapper[LongWritable, Text, Text, LongWritable] {

    var pattern: Pattern = null
    var group: Int = 0

    override def setup(context: Context) {
      pattern = context.getConfiguration().get(PATTERN).r.pattern
      group = context.getConfiguration().getInt(GROUP, 0)
    }

    override def map(key: LongWritable, value: Text, context: Context) {
      val matcher = pattern.matcher(value)
      while (matcher.find()) {
        context.write(matcher.group(group), 1L);
      }
    }
  }

  class LongSumReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce(key: Text, values: Iterator[LongWritable], context: Context) {
      context.write(key, (0L /: values) { (i, j) => i + j.get })
    }
  }

  class InverseMapper extends SMapper[Text, LongWritable, LongWritable, Text] {
    override def map(key: Text, value: LongWritable, context: Context) {
      context.write(value, key)
    }
  }

}
