package tech.mlsql.nativelib.runtime

import org.apache.spark.sql.catalyst.expressions.{Alias, Lower}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * 20/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object NativeFuncRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case Lower(c) => {
        NativeLower(c)
      }
  }

  }
}
