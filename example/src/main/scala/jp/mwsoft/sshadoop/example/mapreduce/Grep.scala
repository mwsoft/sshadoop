package jp.mwsoft.sshadoop.example.mapreduce

import java.util.regex.Pattern
import scala.compat.Platform.currentTime
import org.apache.hadoop.io.LongWritable.DecreasingComparator
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.map.InverseMapper
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import jp.mwsoft.sshadoop.mapreduce.SJob
import jp.mwsoft.sshadoop.mapreduce.SMapper
import jp.mwsoft.sshadoop.util.STool

object Grep extends STool {

  val PATTERN = "mapreduce.mapper.regex"
  val GROUP = "mapreduce.mapper.regexmapper..group"

  def runJob( args: Array[String] ): Boolean = {

    //val args = Array("data/in/shayo.txt", "data/out/grep", ".*文字.*")
    if ( args.size < 3 )
      throw new IllegalArgumentException( "Usage : grep input output pattern" )

    // set pattern and group num
    val conf = getConf()
    conf.set( PATTERN, args( 2 ) )
    if ( args.size > 3 ) conf.setInt( GROUP, args( 3 ).trim.toInt )

    val tempPath = "temp/" + currentTime
    // run grep job
    val grepResult = new SJob( conf, "grep-search" )
      .mapper( new GrepMapper() )
      .reducer( new LongSumReducer[Text]() )
      .outputKeyValueClass( clsText, clsLongWritable )
      .combinerClass( classOf[LongSumReducer[_]] )
      .fileInputPaths( args( 0 ) )
      .fileOutputPath( tempPath )
      .outputFormatClass( classOf[SequenceFileOutputFormat[_, _]] )
      .waitForCompletion( true )

    // run sort job
    grepResult && new SJob( conf, "grep-sort" )
        .numReduceTasks( 1 )
        .mapperClass( classOf[InverseMapper[Text, LongWritable]] )
        .fileInputPaths( tempPath )
        .inputFormatClass( classOf[SequenceFileInputFormat[_, _]] )
        .fileOutputPath( args( 1 ) )
        .sortComparatorClass( classOf[LongWritable.DecreasingComparator] )
        .waitForCompletion( true )
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
}
