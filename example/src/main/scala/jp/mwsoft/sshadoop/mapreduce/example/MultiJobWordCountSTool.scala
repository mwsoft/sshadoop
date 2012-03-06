package jp.mwsoft.sshadoop.mapreduce.example

import scala.compat.Platform.currentTime
import org.apache.hadoop.util.{ Tool, ToolRunner }
import org.apache.hadoop.conf.{ Configured, Configuration }
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapreduce.{ Job, Mapper }
import org.apache.hadoop.mapreduce.lib.map.InverseMapper
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.atilika.kuromoji.Tokenizer
import jp.mwsoft.sshadoop.mapreduce._
import jp.mwsoft.sshadoop.util.STool

/**
 * 日本語用WordCount（数値降順ソート付き）
 */
object WordCount extends STool {

  override def runJob( args: Array[String] ): Boolean = {

    val tempPath = "data/temp/" + currentTime

    // WordCount
    new SJob( getConf(), "wordCount" )
      .mapper( new WordCountMapper() )
      .reducer( new WordCountReducer() )
      .fileInputPaths( "data/in" )
      .fileOutputPath( tempPath )
      .outputFormatClass( classOf[SequenceFileOutputFormat[_, _]] )
      .waitForCompletion( true ) &&
      // 結果の数値降順ソート
      new SJob( getConf(), "resultSort" )
      .mapperClass( classOf[InverseMapper[_, _]] )
      .sortComparatorClass( classOf[LongWritable.DecreasingComparator] )
      .fileInputPaths( tempPath )
      .inputFormatClass( classOf[SequenceFileInputFormat[Text, LongWritable]] )
      .fileOutputPath( "data/out/" + currentTime )
      .waitForCompletion( true )
  }

  /** WordCount用Mapper */
  class WordCountMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
    val tokenizer = Tokenizer.builder().build()
    override def map( key: LongWritable, value: Text, context: Context ) {
      tokenizer.tokenize( value ).iterator() foreach ( x => context.write( x.getSurfaceForm(), 1L ) )
    }
  }

  /** WordCount用Reducer */
  class WordCountReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce( key: Text, values: Iterator[LongWritable], context: Context ) {
      context.write( key, values.map( _.get ).sum )
    }
  }
}
