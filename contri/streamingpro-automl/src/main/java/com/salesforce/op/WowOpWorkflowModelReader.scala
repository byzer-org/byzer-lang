/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.salesforce.op

import com.salesforce.op.OpWorkflowModelReadWriteShared.FieldNames._
import com.salesforce.op.features.{FeatureJsonHelper, OPFeature, TransientFeature}
import com.salesforce.op.stages.{OpPipelineStageReader, _}
import OpPipelineStageReadWriteShared._
import org.apache.spark.ml.util.MLReader
import org.json4s.JsonAST.{JArray, JNothing, JValue}
import org.json4s.jackson.JsonMethods.parse

import scala.util.{Failure, Success, Try}

/**
  * Created by allwefantasy on 19/9/2018.
  */
class WowOpWorkflowModelReader(val workflow: WowOpWorkflow) extends MLReader[OpWorkflowModel] {

  /**
    * Load a previously trained workflow model from path
    *
    * @param path to the trained workflow model
    * @return workflow model
    */
  final override def load(path: String): WowOpWorkflowModel = {
    Try(sc.textFile(OpWorkflowModelReadWriteShared.jsonPath(path), 1).collect().mkString)
      .flatMap(loadJson(_, path = path)) match {
      case Failure(error) => throw new RuntimeException(s"Failed to load Workflow from path '$path'", error)
      case Success(wf) => wf
    }
  }

  /**
    * Load a previously trained workflow model from json
    *
    * @param json json of the trained workflow model
    * @param path to the trained workflow model
    * @return workflow model
    */
  def loadJson(json: String, path: String): Try[WowOpWorkflowModel] = Try(parse(json)).flatMap(loadJson(_, path = path))

  /**
    * Load Workflow instance from json
    *
    * @param json json value
    * @param path to the trained workflow model
    * @return workflow model instance
    */
  def loadJson(json: JValue, path: String): Try[WowOpWorkflowModel] = {
    for {
      trainParams <- OpParams.fromString((json \ TrainParameters.entryName).extract[String])
      params <- OpParams.fromString((json \ Parameters.entryName).extract[String])
      model <- Try(new WowOpWorkflowModel(uid = (json \ Uid.entryName).extract[String], trainParams))
      (stages, resultFeatures) <- Try(resolveFeaturesAndStages(json, path))
      blacklist <- Try(resolveBlacklist(json))
    } yield model
      .setStages(stages.filterNot(_.isInstanceOf[FeatureGeneratorStage[_, _]]))
      .setFeatures(resultFeatures)
      .setParameters(params)
      .setBlacklist(blacklist)
  }

  private def resolveBlacklist(json: JValue): Array[OPFeature] = {
    if ((json \ BlacklistedFeaturesUids.entryName) != JNothing) {
      // for backwards compatibility
      val blacklistIds = (json \ BlacklistedFeaturesUids.entryName).extract[JArray].arr
      val allFeatures = workflow.rawFeatures ++ workflow.blacklistedFeatures ++
        workflow.stages.flatMap(s => s.getInputFeatures()) ++
        workflow.resultFeatures
      blacklistIds.flatMap(uid => allFeatures.find(_.uid == uid.extract[String])).toArray
    } else {
      Array.empty[OPFeature]
    }
  }

  private def resolveFeaturesAndStages(json: JValue, path: String): (Array[OPStage], Array[OPFeature]) = {
    val stages = loadStages(json, path)
    val stagesMap = stages.map(stage => stage.uid -> stage).toMap[String, OPStage]
    val featuresMap = resolveFeatures(json, stagesMap)
    resolveStages(stages, featuresMap)

    val resultIds = (json \ ResultFeaturesUids.entryName).extract[Array[String]]
    val resultFeatures = featuresMap.filterKeys(resultIds.toSet).values

    stages.toArray -> resultFeatures.toArray
  }

  private def loadStages(json: JValue, path: String): Seq[OPStage] = {
    val stagesJs = (json \ Stages.entryName).extract[JArray].arr
    val recoveredStages = stagesJs.map(j => {
      val stageUid = (j \ FieldNames.Uid.entryName).extract[String]
      val originalStage = workflow.stages.find(_.uid == stageUid)
      originalStage match {
        case Some(os) => new WowOpPipelineStageReader(os).loadFromJson(j, path = path).asInstanceOf[OPStage]
        case None => throw new RuntimeException(s"Workflow does not contain a stage with uid: $stageUid")
      }
    })
    val generators = workflow.rawFeatures.map(_.originStage)
    generators ++ recoveredStages
  }

  private def resolveFeatures(json: JValue, stages: Map[String, OPStage]): Map[String, OPFeature] = {
    val results = (json \ AllFeatures.entryName).extract[JArray].arr
    // should have been serialized in topological order
    // so that parent features can be used to construct each new feature
    results.foldLeft(Map.empty[String, OPFeature])((featMap, feat) =>
      FeatureJsonHelper.fromJson(feat, stages, featMap) match {
        case Success(f) => featMap + (f.uid -> f)
        case Failure(e) => throw new RuntimeException(s"Error resolving feature: $feat", e)
      }
    )
  }

  private def resolveStages(stages: Seq[OPStage], featuresMap: Map[String, OPFeature]): Unit = {
    for {stage <- stages} {
      val inputIds = stage.getTransientFeatures().map(_.uid)
      val inFeatures = inputIds.map(id => TransientFeature(featuresMap(id))) // features are order dependent
      stage.set(stage.inputFeatures, inFeatures)
    }
  }


}
