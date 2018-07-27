package streaming.rest

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
  * Created by allwefantasy on 27/7/2018.
  */
trait RestInterceptor {
  def before(request: HttpServletRequest, response: HttpServletResponse)
}
