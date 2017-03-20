package streaming.core.strategy

import java.util

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait ParamsValidator {
  def valid(params: util.Map[Any, Any]): (Boolean, String)
}
