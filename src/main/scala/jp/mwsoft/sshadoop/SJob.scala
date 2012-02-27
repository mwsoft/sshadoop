package jp.mwsoft.sshadoop

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Reducer
import java.net.URI
import org.apache.hadoop.io.RawComparator
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.Configuration

class SJob(conf: Configuration, jobName: String) extends Job(conf, jobName) {

  def this(conf: Configuration) = this(conf, null)
  
  def this(jobName: String) = this(new Configuration(), jobName)

  def this() = this(new Configuration())

  protected var _mapper: Option[SMapper[_, _, _, _]] = None

  protected var _reducer: Option[SReducer[_, _, _, _]] = None

  protected var _combiner: Option[SReducer[_, _, _, _]] = None

  def fileInputPath(path: Path) = { FileInputFormat.setInputPaths(this, path); this }

  def fileInputPath(path: String) = { FileInputFormat.setInputPaths(this, new Path(path)); this }

  def fileOutputPath(path: Path) = { FileOutputFormat.setOutputPath(this, path); this }

  def fileOutputPath(path: String) = { FileOutputFormat.setOutputPath(this, new Path(path)); this }

  def cacheArchives(value: Boolean) = { this.setCancelDelegationTokenUponJobCompletion(value); this }

  def combiner(combinerInst: SReducer[_, _, _, _]) = {
    if (_mapper.isDefined) {
      if (_mapper.get.outputKeyClass != combinerInst.inputKeyClass)
        throw new IllegalArgumentException("combiner input key must be equal to mapper output key")
      if (_mapper.get.outputValueClass != combinerInst.inputValueClass)
        throw new IllegalArgumentException("combiner value key must be equal to mapper output value")
    }
    if (_reducer.isDefined) {
      if (_reducer.get.inputKeyClass != combinerInst.outputKeyClass)
        throw new IllegalArgumentException("combiner output key must be equal to mapper input key")
      if (_reducer.get.inputValueClass != combinerInst.outputValueClass)
        throw new IllegalArgumentException("combiner output value must be equal to mapper input value")
    }
    _combiner = Some(combinerInst)
    this.setCombinerClass(combinerInst.getClass())
    this
  }

  def groupingComparatorClass[T <: RawComparator[_]](cls: Class[T]) = { this.setGroupingComparatorClass(cls); this }

  def inputFormatClass[T <: InputFormat[_, _]](cls: Class[T]) = { this.setInputFormatClass(cls); this }

  def jarByClass(cls: Class[_]) = { this.setJarByClass(cls); this }

  def jobName(name: String) = { this.setJobName(name); this }

  def mapOutputKeyClass(cls: Class[_]) = { this.setMapOutputKeyClass(cls); this }

  def mapOutputValueClass(cls: Class[_]) = { this.setMapOutputValueClass(cls); this }

  def mapper[T <: SMapper[_, _, _, _]](mapperInst: T) = {
    if (_combiner.isDefined) {
      //if( _combiner.get.inputKeyClass != mapper.outputKeyClass )
      //  throw new 
    }

    this.setJarByClass(mapperInst.getClass)
    this.setMapperClass(mapperInst.getClass)
    if (this.getReducerClass() == null) {
      this.setOutputKeyClass(mapperInst.outputKeyClass)
      this.setOutputValueClass(mapperInst.outputValueClass)
    }
    _mapper = Some(mapperInst)

    this
  }

  def numReduceTasks(num: Int) = { this.setNumReduceTasks(num); this }

  def outputFormatClass[T <: OutputFormat[_, _]](cls: Class[T]) = { this.setOutputFormatClass(cls); this }

  def outputKeyClass(cls: Class[_]) = { this.setOutputKeyClass(cls); this }

  def outputValueClass(cls: Class[_]) = { this.setOutputValueClass(cls); this }

  def partitionerClass[T <: Partitioner[_, _]](cls: Class[T]) = { this.setPartitionerClass(cls); this }

  def reducer(reducerInst: SReducer[_, _, _, _]) = {
    this.setReducerClass(reducerInst.getClass())
    this.setOutputKeyClass(reducerInst.outputKeyClass)
    this.setOutputValueClass(reducerInst.outputValueClass)
    this
  }

  def sortComparatorClass[T <: RawComparator[_]](cls: Class[T]) = { this.setSortComparatorClass(cls); this }

  def workingDirectory(path: Path) = { this.setWorkingDirectory(path); this }

}