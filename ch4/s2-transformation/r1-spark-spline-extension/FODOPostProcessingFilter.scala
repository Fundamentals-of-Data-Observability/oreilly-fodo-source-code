package fodo.ch4.transformation

import org.apache.commons.configuration.Configuration

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.producer.model.v1_1.{ExecutionPlan, ReadOperation, WriteOperation}
import za.co.absa.spline.harvester.postprocessing.AbstractPostProcessingFilter

class FODOPostProcessingFilter(conf: Configuration) extends AbstractPostProcessingFilter {
  type Metrics = Map[String, Any]
  type ListOfInputOperations = scala.collection.mutable.MutableList[ReadOperation]
  type OutputOperation = Option[WriteOperation]
  type CachedCtx = (ListOfInputOperations, OutputOperation)

  // not safe
  val cache = scala.collection.mutable.Map.empty[HarvestingContext, CachedCtx]
  def updateCache[T](ctx: HarvestingContext, o:T):T = ???
  
  // convert a plan into a DataFrame (corresponds to a read operation or write)
  import org.apache.spark.sql.SparkWorkaround
  def toDataFrame(plan: LogicalPlan): DataFrame = SparkWorkaround.toDataFrame(SparkSession.active, plan)

  // compute default metrics on the DataFrame, such as count, null rows, min, max, ...
  def defaultDataFrameMetrics(df: DataFrame):Metrics = ??? // use `describe` for example

  // Find the original LogicalPlan related to the `read` of this data source in the `plan`.
  def extractInputMetrics(op:ReadOperation, plan:LogicalPlan):Metrics = ???
  // update `extraInfo`
  def updateInputExtraWithMetrics(op:ReadOperation, metrics: Metrics): ReadOperation = ???
  override def processReadOperation(op: ReadOperation, ctx: HarvestingContext): ReadOperation = 
    updateCache(ctx, updateInputExtraWithMetrics(op, extractInputMetrics(op, ctx.logicalPlan)))

  def extractOutputMetrics(op:WriteOperation, plan:LogicalPlan):Metrics = ???
  // update `extraInfo`
  def updateOutputExtraWithMetrics(op:WriteOperation, metrics: Metrics): WriteOperation = ???
  override def processWriteOperation(op: WriteOperation, ctx: HarvestingContext): WriteOperation = 
    updateCache(ctx, updateOutputExtraWithMetrics(op, extractOutputMetrics(op, ctx.logicalPlan)))

  // collect additional metrics about the plan
  def extractExecutionMetrics(op:ExecutionPlan, plan:LogicalPlan):Metrics = ???
  // update `extraInfo`
  def updateExecutionExtraWithMetrics(op:ExecutionPlan, metrics: Metrics): ExecutionPlan = ???
  // Collect metrics computed for the inputs and outputs cached, then append them to the `extraInfo`
  // So the LineageDispatcher can reassemble everything when the job is done to create the observations
  def updateExecutionExtraWithDataSourceMetrics(op:ExecutionPlan):ExecutionPlan = ???
  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext): ExecutionPlan = ???

}