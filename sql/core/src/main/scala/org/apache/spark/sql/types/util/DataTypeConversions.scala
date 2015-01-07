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

package org.apache.spark.sql.types.util

import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._


protected[sql] object DataTypeConversions {

  /**
   * Returns the equivalent StructField in Scala for the given StructField in Java.
   */
  def toPublicStructField(catalystField: catalyst.types.StructField): StructField = {
    DataType.createStructField(
      catalystField.name,
      toPublicType(catalystField.dataType),
      catalystField.nullable,
      (new MetadataBuilder).withMetadata(catalystField.metadata).build())
  }

  /**
   * Returns the equivalent DataType in Java for the given DataType in Scala.
   */
  def toPublicType(catalystType: catalyst.types.DataType): DataType = catalystType match {
    // We should only have one implementation of the ConcreteCatalystUDT.
    case udtType: ConcreteCatalystUDT[_] => udtType.udt

    case catalyst.types.StringType => DataType.StringType
    case catalyst.types.BinaryType => DataType.BinaryType
    case catalyst.types.BooleanType => DataType.BooleanType
    case catalyst.types.DateType => DataType.DateType
    case catalyst.types.TimestampType => DataType.TimestampType
    case catalyst.types.DecimalType.Fixed(precision, scale) => new DecimalType(precision, scale)
    case catalyst.types.DecimalType.Unlimited => new DecimalType()
    case catalyst.types.DoubleType => DataType.DoubleType
    case catalyst.types.FloatType => DataType.FloatType
    case catalyst.types.ByteType => DataType.ByteType
    case catalyst.types.IntegerType => DataType.IntegerType
    case catalyst.types.LongType => DataType.LongType
    case catalyst.types.ShortType => DataType.ShortType
    case catalyst.types.NullType => DataType.NullType

    case arrayType: catalyst.types.ArrayType => DataType.createArrayType(
        toPublicType(arrayType.elementType), arrayType.containsNull)
    case mapType: catalyst.types.MapType => DataType.createMapType(
        toPublicType(mapType.keyType),
        toPublicType(mapType.valueType),
        mapType.valueContainsNull)
    case structType: catalyst.types.StructType =>
      DataType.createStructType(structType.fields.map(toPublicStructField).toArray)
  }

  def toPublicType(catalystType: catalyst.types.StructType): StructType = {
    DataType.createStructType(catalystType.fields.map(toPublicStructField).toArray)
  }

  def stringToTime(s: String): java.util.Date = {
    if (!s.contains('T')) {
      // JDBC escape string
      if (s.contains(' ')) {
        java.sql.Timestamp.valueOf(s)
      } else {
        java.sql.Date.valueOf(s)
      }
    } else if (s.endsWith("Z")) {
      // this is zero timezone of ISO8601
      stringToTime(s.substring(0, s.length - 1) + "GMT-00:00")
    } else if (s.indexOf("GMT") == -1) {
      // timezone with ISO8601
      val inset = "+00.00".length
      val s0 = s.substring(0, s.length - inset)
      val s1 = s.substring(s.length - inset, s.length)
      if (s0.substring(s0.lastIndexOf(':')).contains('.')) {
        stringToTime(s0 + "GMT" + s1)
      } else {
        stringToTime(s0 + ".0GMT" + s1)
      }
    } else {
      // ISO8601 with GMT insert
      val ISO8601GMT: SimpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSz" )
      ISO8601GMT.parse(s)
    }
  }

  /** Converts Java objects to catalyst rows / types */
  def convertJavaToCatalyst(a: Any, dataType: catalyst.types.DataType): Any = (a, dataType) match {
    case (obj, udt: catalyst.types.UserDefinedType[_]) =>
      ScalaReflection.convertToCatalyst(obj, udt) // Scala type
    case (d: java.math.BigDecimal, _) =>
      catalyst.types.decimal.Decimal(BigDecimal(d))
    case (other, _) => other
  }

  /** Converts Java objects to catalyst rows / types */
  def convertCatalystToJava(a: Any): Any = a match {
    case d: scala.math.BigDecimal => d.underlying()
    case other => other
  }
}
