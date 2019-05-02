package com.wavy.spark.structured.stream.util

import org.apache.spark.sql.types.{DataTypes, StructType}

object JsonSchemaDefinition {

  def struct = new StructType()
    .add("birth_date", DataTypes.TimestampType)
    .add("name", DataTypes.StringType)
    .add("event_time", DataTypes.TimestampType)
    .add("food", DataTypes.StringType)

}
