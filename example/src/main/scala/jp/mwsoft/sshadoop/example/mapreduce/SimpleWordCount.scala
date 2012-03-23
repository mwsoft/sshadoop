package jp.mwsoft.sshadoop.example.mapreduce

import org.apache.hadoop.io._
import jp.mwsoft.sshadoop.mapreduce._
import jp.mwsoft.sshadoop.util.STool

object WordCountSample extends STool {

  override def runJob( args: Array[String] ): Boolean = {
    new SJob( getConf(), "jobName" )
      .mapper( new MyMapper() )
      .reducer( new MyReducer() )
      .combiner( new MyReducer() )
      .fileInputPaths( "data/in" )
      .fileOutputPath( "data/out" )
      .waitForCompletion( true )
  }

  class MyMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
    override def map( key: LongWritable, value: Text, context: Context ) =
      for ( str <- value.split( "\\s" ) ) context.write( str, 1 )
  }

  class MyReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce( key: Text, values: Iterator[LongWritable], context: Context ) =
      context.write( key, values.map( _.get ).sum )
  }
}
