package jp.mwsoft.sshadoop.io

import java.io._
import org.apache.hadoop.io._

abstract class PairWritable[FIRST <: WritableComparable[_], SECOND <: WritableComparable[_]](
  var first: FIRST, var second: SECOND)
    extends WritableComparable[PairWritable[FIRST, SECOND]] {

  protected val firstCls: Class[FIRST] = first.getClass().asInstanceOf[Class[FIRST]]
  protected val secondCls: Class[SECOND] = first.getClass().asInstanceOf[Class[SECOND]]

  def get: (FIRST, SECOND) = (first, second)

  def set(_value1: FIRST, _value2: SECOND) = { first = _value1; second = _value2 }

  override def readFields(in: DataInput) {
    first.readFields(in)
    second.readFields(in)
  }

  override def write(out: DataOutput) {
    first.write(out)
    second.write(out)
  }

  override def hashCode: Int = first.hashCode * 163 + second.hashCode

  override def equals(o: Any): Boolean = {
    if (o.isInstanceOf[PairWritable[_, _]]) {
      val tp = o.asInstanceOf[PairWritable[_, _]]
      first.equals(tp.first) && second.equals(tp.second)
    } else false
  }

  override def toString = first + "\t" + second

  override def compareTo(w: PairWritable[FIRST, SECOND]): Int = {
    val cmp = first.asInstanceOf[Comparable[FIRST]].compareTo(w.first)
    if (cmp != 0) cmp
    else second.asInstanceOf[Comparable[SECOND]].compareTo(w.second)
  }

}