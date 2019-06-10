package org.apache.spark.sql.delta.commands

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.files.DelayedCommitProtocol
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.execution.QueryExecution


trait DeltaCommandsFun {
  protected def normalizeData(metadata: Metadata,
                              data: Dataset[_],
                              partitionCols: Seq[String]): (QueryExecution, Seq[Attribute]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(metadata.schema, data)
    val cleanedData = SchemaUtils.dropNullTypeColumns(normalizedData)
    val queryExecution = if (cleanedData.schema != normalizedData.schema) {
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData.queryExecution
    } else {
      // For streaming workloads, we need to use the QueryExecution created from StreamExecution
      data.queryExecution
    }
    queryExecution -> cleanedData.queryExecution.analyzed.output
  }


  protected def getCommitter(outputPath: Path): DelayedCommitProtocol =
    new DelayedCommitProtocol("delta", outputPath.toString, None)
}
