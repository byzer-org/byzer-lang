package tech.mlsql.dsl.adaptor

import streaming.common.JSONTool
import streaming.dsl._
import streaming.dsl.parser.DSLSQLParser
import tech.mlsql.Stage
import tech.mlsql.dsl.processor.PreProcessListener

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
class StatementAdaptor(preProcessListener: PreProcessListener, f: String => Unit) extends DslAdaptor {
  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    val PREFIX = ctx.getChild(0).getText.toLowerCase()
    val root = preProcessListener.scriptSQLExecListener
    val statement = PREFIX match {
      case "load" =>
        SingleStatement(loadStatement = new LoadAdaptor(root).analyze(ctx))

      case "select" =>
        SingleStatement(selectStatement = new SelectAdaptor(root).analyze(ctx))

      case "save" =>
        SingleStatement(saveStatement = new SaveAdaptor(root).analyze(ctx))

      case "connect" =>
        SingleStatement(connectStatement = new ConnectAdaptor(root).analyze(ctx))
      case "create" =>
        SingleStatement(createStatement = new CreateAdaptor(root).analyze(ctx))
      case "insert" =>
        SingleStatement(insertStatement = new InsertAdaptor(root).analyze(ctx))
      case "drop" =>
        SingleStatement(dropStatement = new DropAdaptor(root).analyze(ctx))
      case "refresh" =>
        SingleStatement(refreshStatement = new RefreshAdaptor(root).analyze(ctx))
      case "set" =>
        SingleStatement(setStatement = new SetAdaptor(root, Stage.physical).analyze(ctx))
      case "train" | "run" | "predict" =>
        SingleStatement(trainStatement = new TrainAdaptor(root).analyze(ctx))
      case "register" =>
        SingleStatement(registerStatement = new RegisterAdaptor(root).analyze(ctx))
      case a if a.startsWith("!") =>
        SingleStatement(commandStatement = new CommandAdaptor(preProcessListener).analyze(ctx))
      case _ => throw new RuntimeException(s"Unknow statement:${PREFIX} ${ctx.getText}")
    }
    preProcessListener.addSingleStatement(statement)
    f(statement.unwrap.asInstanceOf[ {def raw(): String}].raw)
  }
}

class StatementForIncludeAdaptor(preProcessListener: PreProcessIncludeListener) extends DslAdaptor {
  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    preProcessListener.addStatement(currentText(ctx), SCType.Normal)
  }
}

case class SingleStatement(loadStatement: LoadStatement = null,
                           selectStatement: SelectStatement = null,
                           saveStatement: SaveStatement = null,
                           connectStatement: ConnectStatement = null,
                           createStatement: CreateStatement = null,
                           insertStatement: InsertStatement = null,
                           dropStatement: DropStatement = null,
                           refreshStatement: RefreshStatement = null,
                           setStatement: SetStatement = null,
                           trainStatement: TrainStatement = null,
                           registerStatement: RegisterStatement = null,
                           commandStatement: CommandStatement = null,
                           includeStatement: IncludeStatement = null
                          ) {
  def unwrap: AnyRef = {
    if (loadStatement != null) return loadStatement
    if (selectStatement != null) return selectStatement
    if (saveStatement != null) return saveStatement
    if (connectStatement != null) return connectStatement
    if (createStatement != null) return createStatement
    if (insertStatement != null) return insertStatement
    if (dropStatement != null) return dropStatement
    if (refreshStatement != null) return refreshStatement
    if (setStatement != null) return setStatement
    if (trainStatement != null) return trainStatement
    if (registerStatement != null) return registerStatement
    if (commandStatement != null) return commandStatement
    if (includeStatement != null) return includeStatement
    return null
  }

  def toJson = {
    JSONTool.toJsonStr(this.unwrap)
  }
}
