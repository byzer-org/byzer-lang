package streaming.test

import streaming.remote.YarnApplicationState
import streaming.service.YarnRestService
import streaming.remote.YarnControllerE._

/**
  * Created by allwefantasy on 14/7/2017.
  */
object YarnTest {
  def main(args: Array[String]): Unit = {
    YarnRestService.query("master1.uc.host.dxy:8088", (client, rm) => {
      val response = client.app("application_1499754907574_7181").app()

    })
  }
}
