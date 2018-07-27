package streaming.rest

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import net.csdn.common.exception.RenderFinish

/**
  * Created by allwefantasy on 27/7/2018.
  */
class ExampleRestInterceptor extends RestInterceptor {
  override def before(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    if (request.getPathInfo == "/download") {
      val username = request.getParameter("username")
      if (username == null) {
        response.getWriter.write("username is required")
        throw new RenderFinish()
      }
    }
  }
}
