package streaming.common.zk

import net.csdn.ServiceFramwork
import net.csdn.common.logging.Loggers
import net.csdn.common.network.NetworkUtils.StackType
import net.csdn.common.settings.ImmutableSettings
import net.csdn.common.settings.ImmutableSettings._
import streaming.common.ParamsUtil

/**
 * 8/3/16 WilliamZhu(allwefantasy@gmail.com)
 */
object ZkRegister {
  val logger = Loggers.getLogger(classOf[ZkRegister])

  def registerToZk(params: ParamsUtil) = {
    val settingsB: ImmutableSettings.Builder = settingsBuilder()
    settingsB.put(ServiceFramwork.mode + ".zk.conf_root_dir", params.getParam("streaming.zk.conf_root_dir"))
    settingsB.put(ServiceFramwork.mode + ".zk.servers", params.getParam("streaming.zk.servers"))
    val zk = new ZKClient(settingsB.build())
    val client = zk.zkConfUtil.client

    if (!client.exists(ZKConfUtil.CONF_ROOT_DIR)) {
      client.createPersistent(ZKConfUtil.CONF_ROOT_DIR, true);
    }

    if (client.exists(ZKConfUtil.CONF_ROOT_DIR + "/address")) {
      client.delete(ZKConfUtil.CONF_ROOT_DIR + "/address")
      logger.error(s"${ZKConfUtil.CONF_ROOT_DIR} already exits in zookeeper")
    }
    val hostAddress = net.csdn.common.network.NetworkUtils.getFirstNonLoopbackAddress(StackType.IPv4).getHostAddress
    val port = params.getParam("streaming.driver.port", "9003")
    logger.info(s"register ip and port to zookeeper:\n" +
      s"zk=[${params.getParam("streaming.zk.servers")}]\n" +
      s"${ZKConfUtil.CONF_ROOT_DIR}/address=${hostAddress}:${port}")

    client.createEphemeral(ZKConfUtil.CONF_ROOT_DIR + "/address", hostAddress + ":" + port)
    zk
  }
}

class ZkRegister
