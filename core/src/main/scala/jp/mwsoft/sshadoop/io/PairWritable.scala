package jp.mwsoft.sshadoop.io

import scala.math.Ordered
import org.apache.hadoop.io._
import java.io.{ DataInput, DataOutput }

class PairWritable[FIRST, SECOND](
  var first: WritableComparable[FIRST], var second: WritableComparable[SECOND])
    extends WritableComparable[PairWritable[WritableComparable[FIRST], WritableComparable[SECOND]]] {

  def get = (first, second)

  override def write(out: DataOutput) {
    first.write(out)
    second.write(out)
  }

  override def readFields(in: DataInput) {
    first.readFields(in)
    second.readFields(in)
  }

  override def hashCode = first.hashCode * 163 + second.hashCode

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[PairWritable[FIRST, SECOND]]) false
    else {
      val pw = o.asInstanceOf[PairWritable[_, _]]
      first.equals(pw.first) && second.equals(pw.second)
    }
  }

  override def compareTo(pw: PairWritable[FIRST, SECOND]): Int = {
    val cmp = first compareTo pw
    //if (cmp != 0) cmp
    //else second compareTo pw.second

    1
  }

  override def toString: String = first.toString + "\t" + second.toString
}
