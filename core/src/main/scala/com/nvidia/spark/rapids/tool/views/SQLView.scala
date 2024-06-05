/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tool.views

import com.nvidia.spark.rapids.tool.analysis.{AppSQLPlanAnalyzer, ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.{SQLAccumProfileResults, SQLCleanAndAlignIdsProfileResult, SQLPlanClassifier, WholeStageCodeGenResults}

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

trait AppSQLCodeGenViewTrait extends ViewableTrait[WholeStageCodeGenResults] {
  override def getLabel: String = "WholeStageCodeGen Mapping"
  override def getDescription: String = "WholeStagecodeGen Mapping"

  override def sortView(
      rows: Seq[WholeStageCodeGenResults]): Seq[WholeStageCodeGenResults] = {
    rows.sortBy(cols => (cols.appIndex, cols.sqlID, cols.nodeID))
  }
}

trait AppSQLPlanMetricsViewTrait extends ViewableTrait[SQLAccumProfileResults] {
  override def getLabel: String = "SQL Plan Metrics for Application"
  override def getDescription: String = "SQL Plan Metrics"

  override def sortView(
      rows: Seq[SQLAccumProfileResults]): Seq[SQLAccumProfileResults] = {
    rows.sortBy(cols => (cols.appIndex, cols.sqlID, cols.nodeID,
      cols.nodeName, cols.accumulatorId, cols.metricType))
  }
}

trait AppSQLPlanNonDeltaOpsViewTrait extends ViewableTrait[SQLCleanAndAlignIdsProfileResult] {
  override def getLabel: String = "SQL Ids Cleaned For Alignment"
  override def getDescription: String = "SQL Ids Cleaned For Alignment"

  override def sortView(
      rows: Seq[SQLCleanAndAlignIdsProfileResult]): Seq[SQLCleanAndAlignIdsProfileResult] = {
    rows.sortBy(cols => (cols.appIndex, cols.sqlID))
  }
}

/**
 * This view is meant to clean up Delta log execs so that you could align
 * SQL ids between CPU and GPU eventlogs. It attempts to remove any delta log
 * SQL ids. This includes reading checkpoints, delta_log json files,
 * updating Delta state cache/table.
 */
object ProfSQLPlanAlignedView extends AppSQLPlanNonDeltaOpsViewTrait with ProfAppIndexMapperTrait {
  override def getRawView(app: AppBase, index: Int): Seq[SQLCleanAndAlignIdsProfileResult] = {
    // Create a SQLClassifier object and attach it to the AppInfo
    val sqlPlanTypeAnalysis = new SQLPlanClassifier(app.asInstanceOf[ApplicationInfo])
    // Walk through the SQLPlans to identify the delta-op SQIds
    sqlPlanTypeAnalysis.walkPlans(app.sqlPlans)
    app.sqlPlans.filterKeys(!sqlPlanTypeAnalysis.sqlCategories("deltaOp").contains(_)).
      map { case (sqlID, _) =>
        SQLCleanAndAlignIdsProfileResult(index, sqlID)
      }.toSeq
  }
}

object ProfSQLCodeGenView extends AppSQLCodeGenViewTrait with ProfAppIndexMapperTrait {

  override def getRawView(app: AppBase, index: Int): Seq[WholeStageCodeGenResults] = {
    app match {
      case app: ApplicationInfo =>
        app.planMetricProcessor.wholeStage
      case _ => Seq.empty
    }
  }
}

object QualSQLCodeGenView extends AppSQLCodeGenViewTrait with QualAppIndexMapperTrait {

  override def getRawView(app: AppBase, index: Int): Seq[WholeStageCodeGenResults] = {
    // TODO: Fix this implementation when we have a better way to get bind between App and
    //  SQLProcessor but for now, we do not store SQLPlan Processor in QualificationSummaryInfo
    // to save
    Seq.empty
  }

  def getRawViewFromSqlProcessor(
      sqlAnalyzer: AppSQLPlanAnalyzer): Seq[WholeStageCodeGenResults] = {
    sortView(sqlAnalyzer.wholeStage)
  }
}

object ProfSQLPlanMetricsView extends AppSQLPlanMetricsViewTrait with ProfAppIndexMapperTrait {

  override def getRawView(app: AppBase, index: Int): Seq[SQLAccumProfileResults] = {
    app match {
      case app: ApplicationInfo =>
        app.planMetricProcessor.generateSQLAccums()
      case _ => Seq.empty
    }
  }
}

object QualSQLPlanMetricsView extends AppSQLPlanMetricsViewTrait with QualAppIndexMapperTrait {
  override def getRawView(app: AppBase, index: Int): Seq[SQLAccumProfileResults] = {
    // TODO: Fix this implementation when we have a better way to get bind between App and
    //  SQLProcessor but for now, we do not store SQLPlan Processor in QualificationSummaryInfo
    // to save
    Seq.empty
  }

  def getRawViewFromSqlProcessor(
      sqlAnalyzer: AppSQLPlanAnalyzer): Seq[SQLAccumProfileResults] = {
    sortView(sqlAnalyzer.generateSQLAccums())
  }
}
