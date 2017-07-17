package streaming.db

import java.io.File
import java.util.Date

import net.liftweb.{json => SJSon}
import org.squeryl.PrimitiveTypeMode._
import org.squeryl._
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.annotations.Column

import scala.io.Source

/**
  * Created by allwefantasy on 12/7/2017.
  */
class TTestConf(val id: Long, var name: String, var user: String, var url: String, var host: String, @Column("insert_time") var insertTime: Date) extends KeyedEntity[Long] {
  def this(name: String, user: String, url: String, host: String) = this(0, name, user, url, host, new Date())
}

class TParamsConf(val id: Long, var params: String) extends KeyedEntity[Long]

class TSparkApplication(val id: Long,
                        var applicationId: String,
                        var parentApplicationId: String,
                        var url: String,
                        var source: String,
                        var beforeShell: String,
                        var afterShell: String,
                        var keepRunning: Int,
                        var watchInterval: Int,
                        var startTime: Long,
                        var endTime: Long
                       ) extends KeyedEntity[Long]

class TSparkApplicationLog(val id: Long,
                           var applicationId: String,
                           var url: String,
                           var source: String,
                           var parentApplicationId: String,
                           var failReason: String,
                           var startTime: Long,
                           var endTime: Long
                          ) extends KeyedEntity[Long]

class TSparkJar(val id: Long,
                var name: String,
                var path: String,
                var createTime: Long
               ) extends KeyedEntity[Long]

object TSparkJar {
  def find(id: Long) = {
    transaction {
      from(DB.tSparkJar)(s => where(s.id === id) select (s)).singleOption
    }
  }

  def findByName(name: String) = {
    transaction {
      from(DB.tSparkJar)(s => where(s.name === name) select (s)).singleOption
    }
  }

  def save(item: TSparkJar) = {
    //implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    transaction {
      DB.tSparkJar.insert(item)
    }
  }

  def reSave(app: TSparkJar) = {
    transaction {
      DB.tSparkJar.update(app)
    }
  }

  def delete(id: Long) = {
    transaction {
      DB.tSparkJar.delete(id)
    }
  }

  def list = {
    transaction {
      from(DB.tSparkJar)(s => select(s)).toList
    }
  }
}

object TSparkApplication extends PrimitiveTypeMode {

  def saveLog(appLog: TSparkApplicationLog) = {
    transaction {
      DB.tSparkApplicationLog.insert(appLog)
    }
  }

  /*
    监控周期的功能现在还没有加上
   */
  def shouldWatch(app: TSparkApplication) = {
    app.keepRunning == 0 &&
      app.watchInterval > 0 &&
      app.applicationId != null &&
      !app.applicationId.isEmpty
  }

  val KEEP_RUNNING = 0
  val NO_KEEP_RUNNING = 1
  val WATCH_INTERVAL = 1000
  val NO_WATCH_INTERVAL = -1

  def list = {
    transaction {
      from(DB.tSparkApplication)(s => select(s)).toList
    }
  }

  def queryLog = {
    transaction {
      from(DB.tSparkApplicationLog)(s => select(s)).toList
    }
  }

  def find(id: Long) = {
    transaction {
      from(DB.tSparkApplication)(s => where(s.id === id) select (s)).singleOption
    }
  }

  def save(applicationId: String, url: String, source: String, beforeShell: String, afterShell: String) = {
    //implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    val temp = new TSparkApplication(0, "", "", url, source, beforeShell, afterShell, 1, -1, System.currentTimeMillis(), 0)
    transaction {
      DB.tSparkApplication.insert(temp)
    }
  }

  def reSave(app: TSparkApplication) = {
    transaction {
      DB.tSparkApplication.update(app)
    }
  }

  def delete(id: Long) = {
    transaction {
      DB.tSparkApplication.delete(id)
    }
  }
}

object TParamsConf extends PrimitiveTypeMode {
  def find(id: Long) = {
    implicit val formats = SJSon.DefaultFormats

    transaction {
      from(DB.tParamsConf)(s => where(s.id === id) select (s)).singleOption match {
        case Some(i) =>
          SJSon.parse(i.params).extract[Map[String, String]]
        case None => Map[String, String]()
      }
    }
  }

  def save(params: Map[String, String]) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    val temp = new TParamsConf(0, SJSon.Serialization.write(params))
    transaction {
      DB.tParamsConf.insert(temp)
    }
  }

  def reSave(id: Long, params: Map[String, String]) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    transaction {
      update(DB.tParamsConf)(s =>
        where(s.id === id)
          set (s.params := SJSon.Serialization.write(params)))
    }
  }
}

object TTestConf extends PrimitiveTypeMode {
  def find(name: String) = {
    transaction {
      from(DB.tTestConf)(s => where(s.name === name) select (s)).singleOption
    }
  }

  def list = {
    transaction {
      from(DB.tTestConf)(s => select(s)).toList
    }
  }


  def updateConf(tTestConf: TTestConf) = {
    transaction {
      update(DB.tTestConf)(s =>
        where(s.name === tTestConf.name)
          set(s.host := tTestConf.host, s.url := tTestConf.url, s.user := tTestConf.user))
    }
  }
}

object DB extends Schema {


  def parseConfig = {
    var config = Map[String, String]()

    if (ManagerConfiguration.config.hasParam("jdbcPath")) {
      val path = ManagerConfiguration.config.getParam("jdbcPath")

      val configLines = path match {
        case i if i.startsWith("classpath://") =>
          Source.fromInputStream(DB.getClass.getResourceAsStream(path.substring("classpath://".length))).getLines()
        case i if i.startsWith("/") =>
          Source.fromFile(new File(i)).getLines()

      }

      config ++= configLines.map {
        f =>
          val abc = f.split("=")
          if (abc.length == 2) (abc(0).trim, abc(1).trim)
          else (abc(0), abc.takeRight(abc.length - 1).mkString("="))
      }.toMap
    }

    if (ManagerConfiguration.config.hasParam("jdbc.url")) {
      config += ("url" -> ManagerConfiguration.config.getParam("jdbc.url"))

      if (ManagerConfiguration.config.hasParam("jdbc.userName")) {
        config += ("userName" -> ManagerConfiguration.config.getParam("jdbc.userName"))
      }
      if (ManagerConfiguration.config.hasParam("jdbc.password")) {
        config += "password" -> ManagerConfiguration.config.getParam("jdbc.password")
      }

    }
    config
  }

  val jdbcConfig = parseConfig

  Class.forName("com.mysql.jdbc.Driver");

  SessionFactory.concreteFactory = Some(() =>
    Session.create(
      java.sql.DriverManager.getConnection(jdbcConfig("url"), jdbcConfig("userName"), jdbcConfig("password")),
      new MySQLAdapter)
  )
  val tTestConf = table[TTestConf]("t_test_conf")
  val tParamsConf = table[TParamsConf]("t_params_conf")
  val tSparkApplication = table[TSparkApplication]("t_spark_application")
  val tSparkApplicationLog = table[TSparkApplicationLog]("t_spark_application_log")
  val tSparkJar = table[TSparkJar]("t_spark_jar")


}

object Test {
  def main(args: Array[String]): Unit = {
    DB
    TTestConf.find("java") match {
      case Some(i) =>
      case None =>
        transaction {
          DB.tTestConf.insert(new TTestConf("java", "webuser", "url", "mmmm"))
        }

    }
  }
}
