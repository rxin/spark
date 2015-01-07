/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types

import java.io.Serializable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst


/**
 * The data type representing Lists. To create an [[ArrayType]], use [[DataType.createArrayType()]].
 *
 * @param elementType Type of the array elements.
 * @param containsNull Whether the array has null values.
 */
case class ArrayType protected(elementType: DataType, containsNull: Boolean) extends DataType {
  override def toCatalyst = new catalyst.types.ArrayType(elementType.toCatalyst, containsNull)
}


/**
 * The data type representing byte array values.
 * Represented by the singleton object [[DataType.BinaryType]].
 */
case object BinaryType extends DataType {
  override def toCatalyst = catalyst.types.BinaryType
}


/**
 * The data type representing boolean values.
 * Represented by the singleton object [[DataType.BooleanType]].
 */
case object BooleanType extends DataType {
  override def toCatalyst = catalyst.types.BooleanType
}


/**
 * The data type representing byte values.
 * Represented by the singleton object [[DataType.ByteType]].
 */
case object ByteType extends DataType {
  override def toCatalyst = catalyst.types.ByteType
}


/**
 * The data type representing java.sql.Date values.
 * Represented by the singleton object [[DataType.DateType]].
 */
case object DateType extends DataType {
  override def toCatalyst = catalyst.types.DateType
}


/**
 * The data type representing java.math.BigDecimal values.
 *
 * @param hasPrecisionInfo whether we have precision set or not.
 * @param precision precision, or -1 if no precision is set.
 * @param scale scale, or -1 if no precision is set.
 */
case class DecimalType (hasPrecisionInfo: Boolean, precision: Int, scale: Int)
  extends DataType {

  def this(precision: Int, scale: Int) {
    this(hasPrecisionInfo = true, precision, scale)
  }

  def this() {
    this(hasPrecisionInfo = false, -1, -1)
  }

  override def toCatalyst = {
    if (isFixed) {
      catalyst.types.DecimalType(precision, scale)
    } else {
      catalyst.types.DecimalType.Unlimited
    }
  }

  def isUnlimited: Boolean = !hasPrecisionInfo

  def isFixed: Boolean = hasPrecisionInfo
}


/**
 * The data type representing 64-bit floating point (double) values.
 * Represented by the singleton object [[DataType.DoubleType]].
 */
case object DoubleType extends DataType {
  override def toCatalyst = catalyst.types.DoubleType
}


/**
 * The data type representing 32-bit floating point values.
 * Represented by the singleton object [[DataType.FloatType]].
 */
case object FloatType extends DataType {
  override def toCatalyst = catalyst.types.FloatType
}


/**
 * The data type representing integer values.
 * Represented by the singleton object [[DataType.IntegerType]].
 */
case object IntegerType extends DataType {
  override def toCatalyst = catalyst.types.IntegerType
}


/**
 * The data type representing long values.
 * Represented by the singleton object [[DataType.LongType]].
 */
case object LongType extends DataType {
  override def toCatalyst = catalyst.types.LongType
}


/**
 * The data type representing Maps. Keys are not allowed to have null values.
 *
 * To create a [[MapType]], use [[DataType.createMapType()]].
 *
 * @param keyType type of keys in the map.
 * @param valueType type of values in the map.
 * @param valueContainsNull whether the values contain null.
 */
case class MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
  extends DataType {

  override def toCatalyst = {
    catalyst.types.MapType(keyType.toCatalyst, valueType.toCatalyst, valueContainsNull)
  }
}


/**
 * The data type representing null values.
 * Represented by the singleton object [[DataType.NullType]].
 */
case object NullType extends DataType {
  override def toCatalyst = catalyst.types.NullType
}


/**
 * The data type representing short values.
 * Represented by the singleton object [[DataType.ShortType]].
 */
case object ShortType extends DataType {
  override def toCatalyst = catalyst.types.ShortType
}


/**
 * The data type representing string values.
 * Represented by the singleton object [[DataType.StringType]].
 */
case object StringType extends DataType {
  override def toCatalyst = catalyst.types.StringType
}


/**
 * A StructField object represents a field in a StructType object.
 *
 * @param name name of the field.
 * @param dataType type of the field.
 * @param nullable whether the value can be null.
 * @param metadata extra information about the fifeld.
 */
case class StructField(name: String, dataType: DataType, nullable: Boolean, metadata: Metadata) {
  def toCatalyst: catalyst.types.StructField = {
    catalyst.types.StructField(name, dataType.toCatalyst, nullable, metadata)
  }
}


/**
 * The data type representing Rows. A StructType object comprises an array of StructFields.
 *
 * To create an [[StructType]], use [[DataType#createStructType()]].
 */
case class StructType protected(fields: Array[StructField]) extends DataType {

  override def toCatalyst = {
    catalyst.types.StructType(fields.map { f =>
      catalyst.types.StructField(f.name, f.dataType.toCatalyst, f.nullable, f.metadata)
    })
  }

  override def equals(other: Any): Boolean = other match {
    case StructType(otherFields) =>
      java.util.Arrays.equals(
        fields.asInstanceOf[Array[AnyRef]], otherFields.asInstanceOf[Array[AnyRef]])
    case _ =>
      false
  }

  override def hashCode: Int = java.util.Arrays.hashCode(fields.asInstanceOf[Array[AnyRef]])

  /**
   * Extracts a [[StructField]] of the given name. If the [[StructType]] object does not
   * have a name matching the given name, `null` will be returned.
   */
  def apply(name: String): StructField = {
    fields.find(_.name == name).getOrElse(
      throw new IllegalArgumentException(s"Field $name does not exist."))
  }

  def treeString: String = toCatalyst.treeString
}


/**
 * The data type representing string values.
 * Represented by the singleton object [[DataType.TimestampType]].
 */
case object TimestampType extends DataType {
  override def toCatalyst = catalyst.types.TimestampType
}


/**
 * ::DeveloperApi::
 * The data type representing User-Defined Types (UDTs).
 * UDTs may use any other DataType for an underlying representation.
 */
@DeveloperApi
abstract class UserDefinedType[UserType] extends DataType with Serializable {

  override def toCatalyst = new ConcreteCatalystUDT[UserType](this)

  override def equals(o: Any): Boolean = o match {
    case obj: UserDefinedType[_] =>
      getClass == obj.getClass && sqlType == obj.sqlType
    case _ =>
      false
  }

  /** Underlying storage type for this UDT */
  def sqlType: DataType

  /** Convert the user type to a SQL datum */
  def serialize(obj: Any): Any

  /** Convert a SQL datum to the user type */
  def deserialize(datum: Any): UserType

  /** Class object for the UserType */
  def userClass: Class[UserType]
}


/**
 * A concrete implementation of the Catalyst UDT that references a public UDT.
 */
private[types] class ConcreteCatalystUDT[UserType](val udt: UserDefinedType[UserType])
  extends catalyst.types.UserDefinedType[UserType] {

  override def sqlType: catalyst.types.DataType = udt.sqlType.toCatalyst
  override def serialize(obj: Any): Any = udt.serialize(obj)
  override def userClass: Class[UserType] = udt.userClass
  override def deserialize(datum: Any): UserType = udt.deserialize(datum)
}
