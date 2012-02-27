package jp.mwsoft.sshadoop

import org.apache.hadoop.io.WritableComparator
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text

abstract class SWritableComparator[T <: WritableComparable[T] with Writable](implicit keyOutType: Manifest[T])
  extends WritableComparator(keyOutType.erasure.asInstanceOf[Class[T]])
  with ImplicitConversions
