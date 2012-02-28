package jp.mwsoft.sshadoop.mapreduce.example

import scala.compat.Platform.currentTime
import jp.mwsoft.sshadoop.mapreduce._
import org.apache.hadoop.io.{ Text, LongWritable }
import java.util.regex.Pattern
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

object Grep extends STool {

  val PATTERN = "mapreduce.mapper.regex"
  val GROUP = "mapreduce.mapper.regexmapper..group"

  def runJob( args: Array[String] ): Boolean = {

    val args = Array( "data/in/shayo2.txt", "data/out/grep", ".*お母さま.*" )

    if ( args.size < 3 )
      throw new IllegalArgumentException( "Usage : grep input output pattern" )

    val conf = getConf()
    conf.set( PATTERN, args( 2 ) )
    if ( args.size > 3 ) conf.setInt( GROUP, args( 3 ).trim.toInt )

    val tempPath = "temp/" + currentTime

    // run grep job
    val grepResult = new SJob( conf, "grep-search" )
      .mapper( new GrepMapper() )
      .combiner( new LongSumReducer() )
      .reducer( new LongSumReducer() )
      .fileInputPath( args( 0 ) )
      .fileOutputPath( tempPath )
      .outputFormatClass( classOf[SequenceFileOutputFormat[_, _]] )
      .waitForCompletion( true )

    // run sort job
    if ( grepResult )
      new SJob( conf, "grep-sort" )
        .mapper( new InverseMapper() )
        .numReduceTasks( 1 )
        .fileInputPath( tempPath )
        .inputFormatClass( classOf[SequenceFileInputFormat[_, _]] )
        .fileOutputPath( args( 1 ) )
        .sortComparatorClass( classOf[LongWritable.DecreasingComparator] )
        .waitForCompletion( true )
    else false
  }

  class GrepMapper extends SMapper[LongWritable, Text, Text, LongWritable] {

    var pattern: Pattern = null
    var group: Int = 0

    override def setup( context: Context ) {
      pattern = context.getConfiguration().get( PATTERN ).r.pattern
      group = context.getConfiguration().getInt( GROUP, 0 )
    }

    override def map( key: LongWritable, value: Text, context: Context ) {
      val matcher = pattern.matcher( value )
      while ( matcher.find() ) {
        context.write( matcher.group( group ), 1L );
      }
    }
  }

  class LongSumReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce( key: Text, values: Iterator[LongWritable], context: Context ) {
      context.write( key, ( 0L /: values ) { ( i, j ) => i + j.get } )
    }
  }

  class InverseMapper extends SMapper[Text, LongWritable, LongWritable, Text] {
    override def map( key: Text, value: LongWritable, context: Context ) {
      context.write( value, key )
    }
  }

}
