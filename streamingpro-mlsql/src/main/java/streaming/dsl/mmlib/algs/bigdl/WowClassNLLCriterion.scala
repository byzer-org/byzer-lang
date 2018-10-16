package streaming.dsl.mmlib.algs.bigdl

import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric


object WowClassNLLCriterion {
  def apply(
             paramsExtractor: ClassWeightParamExtractor
           )(implicit ev: TensorNumeric[Float]): ClassNLLCriterion[Float] = {
    val weights = paramsExtractor.weights.map(f => Tensor(f, Array(f.size))).getOrElse(null)
    new ClassNLLCriterion[Float](weights,
      paramsExtractor.sizeAverage.getOrElse(true),
      paramsExtractor.logProbAsInput.getOrElse(true),
      paramsExtractor.paddingValue.getOrElse(-1)
    )
  }
}
