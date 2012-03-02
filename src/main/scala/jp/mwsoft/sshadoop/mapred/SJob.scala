package jp.mwsoft.sshadoop.mapred

import org.apache.hadoop.mapred.jobcontrol.Job
import org.apache.hadoop.mapred.JobConf
import java.util.ArrayList
import org.apache.hadoop.conf.Configuration

class SJob(conf: JobConf, dependingJobs: ArrayList[Job]) extends Job(conf, dependingJobs) {

  def this(conf: JobConf) = this(conf, null)

}