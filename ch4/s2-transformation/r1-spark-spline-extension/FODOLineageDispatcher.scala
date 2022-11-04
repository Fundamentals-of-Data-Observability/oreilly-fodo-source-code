package fodo.ch4.transformation

import org.apache.spark.internal.Logging

import org.apache.commons.configuration.Configuration
import za.co.absa.commons.config.ConfigurationImplicits._

import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.producer.model.v1_1.ExecutionEvent
import za.co.absa.spline.producer.model.v1_1.ExecutionPlan

class FODOLineageDispatcher(conf: Configuration) extends LineageDispatcher with Logging {
    
  type DataObservationBase = Any
  type Observations = List[DataObservationBase]

  val client:Any = ??? // create client to publish data observations using configuration

  // navigate the plan to convert it into the core model
  // inluding the extraInfo where is hidden the information about the input and output
  def convertIntoDataObservations(plan:ExecutionPlan):Observations = ??? // use client to publish

  // Send observations to the plaform, file, ... anything that can be used to aggregate them
  def reportDataObservations(obs: Observations):Unit = ??? 

  override def send(plan: ExecutionPlan): Unit = 
    reportDataObservations(convertIntoDataObservations(plan))
  

  override def send(event: ExecutionEvent): Unit = ???

}