package streaming.dsl.mmlib.algs.bigdl

import com.intel.analytics.bigdl.optim.{Loss, _}
import streaming.common.ScalaObjectReflect
import streaming.dsl.mmlib.algs.SQLBigDLClassifyExt
import streaming.session.MLSQLException
import org.json4s._
import org.json4s.jackson.JsonMethods

class SummaryParamExtractor(bigDLClassifyExt: SQLBigDLClassifyExt, _params: Map[String, String]) extends BaseExtractor {
  def summaryTrainDir = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.summary_trainDir.name)).map(f => f._2).headOption
  }

  def summaryValidateDir = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.summary_validateDir.name)).map(f => f._2).headOption
  }
}

class OptimizeParamExtractor(bigDLClassifyExt: SQLBigDLClassifyExt, _params: Map[String, String]) extends BaseExtractor {
  def optimizeMethod = {
    val methods = _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.optimizeMethod.name)).map { f =>
      f._2
    }.toArray
    OptimizeParamExtractor.extractOptimizeMethods(methods).headOption
  }
}

object OptimizeParamExtractor {
  private[bigdl] val optimizeMethodCandidates = List(
    classOf[Adam[Float]],
    classOf[Adamax[Float]],
    classOf[Adadelta[Float]],
    classOf[Ftrl[Float]],
    classOf[Top5Accuracy[Float]],
    classOf[LBFGS[Float]],
    classOf[RMSprop[Float]],
    classOf[SGD[Float]]
  )

  def extractOptimizeMethods(methods: Array[String]) = {
    val filterSet = optimizeMethodCandidates.map(f => f.getSimpleName).toSet
    val notExistMethods = methods.filterNot(f => filterSet.contains(f))
    if (notExistMethods.size > 0) {
      throw new MLSQLException(
        s"""
           |${notExistMethods.mkString(",")} are not exits.
           |${EvaluateParamsExtractor.helperStr}
         """.stripMargin)
    }
    methods.map { name =>
      import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
      val candiate = optimizeMethodCandidates.filter(c => c.getSimpleName == name).head
      candiate.getConstructor(classOf[TensorNumeric[Float]]).newInstance(TensorNumeric.NumericFloat).asInstanceOf[OptimMethod[Float]]
    }.toSeq
  }

  def optimizeMethodCandidatesStr = {
    optimizeMethodCandidates.map(f => f.getSimpleName).mkString("|")
  }

}

class ClassWeightParamExtractor(bigDLClassifyExt: SQLBigDLClassifyExt, _params: Map[String, String]) extends BaseExtractor {
  def weights = {
    implicit val formats = DefaultFormats
    val classWeight = _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.criterion_classWeight.name)).headOption
    val res = classWeight.map(f => JsonMethods.parse(f._2).extract[Array[Float]]).headOption
    if (classNum.isDefined && res.isDefined) {
      if (classNum.head != res.head.size) {
        throw new MLSQLException(s"classWeight should have the same size with classNum(${classNum.get})")
      }
    }
    res
  }

  def classNum = {
    _params.filter(f => f._1 == "classNum").map(f => f._2.toInt).headOption
  }

  def sizeAverage = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.criterion_sizeAverage.name)).map(f => f._2.toBoolean).headOption
  }

  def logProbAsInput = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.criterion_logProbAsInput.name)).map(f => f._2.toBoolean).headOption
  }

  def paddingValue = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.criterion_paddingValue.name)).map(f => f._2.toInt).headOption
  }
}

class EvaluateParamsExtractor(bigDLClassifyExt: SQLBigDLClassifyExt, _params: Map[String, String]) extends BaseExtractor {
  val bigDLClassifyExtParams = bigDLClassifyExt.params


  private[bigdl] def trigger = {
    _params.filter(f => f._1.startsWith("evaluate.trigger.")).map { f =>
      if (bigDLClassifyExtParams.filter(m => cleanGroupPrefix(m.name) == f._1).size == 0) {
        throw new MLSQLException(
          s"""
             |${f._1} is not recognized by BigDLClassifyExt
             |
             |${EvaluateParamsExtractor.helperStr}
           """.stripMargin)
      }
      val triggerType = f._1.split("\\.").last
      val (clzz, instance) = ScalaObjectReflect.findObjectMethod(Trigger.getClass.getName.stripSuffix("$"))
      val method = clzz.getDeclaredMethods.filter(f => f.getName == triggerType).head
      if (method.getParameterTypes.size == 0) {
        method.invoke(instance).asInstanceOf[Trigger]
      } else {
        // for now, trigger only have 0-1 parameters
        val pt = method.getParameterTypes.head
        val v2 = pt match {
          case i if i.isAssignableFrom(classOf[Int]) => f._2.toInt
          case i if i.isAssignableFrom(classOf[Double]) => f._2.toDouble
          case i if i.isAssignableFrom(classOf[Float]) => f._2.toFloat
          case i if i.isAssignableFrom(classOf[Boolean]) => f._2.toBoolean
          case i if i.isAssignableFrom(classOf[String]) => f._2
        }
        method.invoke(instance, v2.asInstanceOf[AnyRef]).asInstanceOf[Trigger]
      }

    }.headOption
  }

  private[bigdl] def evaluateTable = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.evaluate_table.name)).map(f => f._2).headOption
  }

  private[bigdl] def evaluateMethods = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.evaluate_methods.name)).map { f =>
      val methods = f._2.split(",")
      EvaluateParamsExtractor.extractEvaluateMethods(methods)
    }.headOption
  }

  private[bigdl] def batchSize = {
    _params.filter(f => f._1 == cleanGroupPrefix(bigDLClassifyExt.evaluate_batchSize.name)).map(f => f._2.toInt).headOption
  }


  def bigDLEvaluateConfig = {
    BigDLEvaluateConfig(trigger, evaluateTable, evaluateMethods.map(f => f.toArray), batchSize)
  }

  def defaultEvaluateMethods = Array(
    new Top1Accuracy[Float](), new Loss[Float]()
  )
}

trait BaseExtractor {
  private[bigdl] def cleanGroupPrefix(str: String) = {
    str.split("\\.").splitAt(2)._2.mkString(".")
  }

}

object ClassWeightParamExtractor {

}

object EvaluateParamsExtractor {
  private[bigdl] val evaluateMethodCandidates = List(
    classOf[Loss[Float]],
    classOf[Top1Accuracy[Float]],
    classOf[MAE[Float]],
    classOf[Top5Accuracy[Float]],
    classOf[TreeNNAccuracy[Float]]
  )

  def extractEvaluateMethods(methods: Array[String]) = {
    val filterSet = evaluateMethodCandidates.map(f => f.getSimpleName).toSet
    val notExistMethods = methods.filterNot(f => filterSet.contains(f))
    if (notExistMethods.size > 0) {
      throw new MLSQLException(
        s"""
           |${notExistMethods.mkString(",")} are not exits.
           |${helperStr}
         """.stripMargin)
    }
    methods.map { name =>
      import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
      evaluateMethodCandidates.filter(c => c.getSimpleName == name).head match {

        case a if a.isAssignableFrom(classOf[Loss[Float]]) => new Loss[Float]()
        case a: Class[_] => a.getConstructor(classOf[TensorNumeric[Float]]).newInstance(TensorNumeric.NumericFloat)

      }
    }.toSeq
  }

  def evaluateMethodCandidatesStr = {
    evaluateMethodCandidates.map(f => f.getSimpleName).mkString("|")
  }


  val helperStr =
    s"""
       |Please use load statement to check params detail:
       |
       |```
       |load modelParams.`BigDLClassifyExt` as output;
       |```
     """.stripMargin
}

case class BigDLEvaluateConfig(trigger: Option[Trigger], validationTable: Option[String],
                               vMethods: Option[Array[ValidationMethod[Float]]], batchSize: Option[Int])
