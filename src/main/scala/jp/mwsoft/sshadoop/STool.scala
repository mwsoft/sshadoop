package jp.mwsoft.sshadoop

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{ Tool, ToolRunner }

trait STool extends Configured with Tool {

  def main( args: Array[String] ) {
    exit( ToolRunner.run( this, args ) )
  }

  override def run( args: Array[String] ): Int = if ( runJob( args ) ) 0 else 1

  def runJob( args: Array[String] ): Boolean
}