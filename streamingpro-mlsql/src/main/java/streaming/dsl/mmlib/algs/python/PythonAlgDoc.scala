package streaming.dsl.mmlib.algs.python

import streaming.dsl.mmlib.{Doc, MarkDownDoc}

object PythonAlgDoc {
  def doc = {
    Doc(MarkDownDoc,
      s"""
         |
         |Requirements:
         |
         |1. Conda is installed in your cluster.
         |2. The user who runs StreamingPro cluster has the permission to read/write `/tmp/__mlsql__`.
         |
         |Suppose you run StreamingPro/MLSQL with user named `mlsql`.
         |Conda should be installed by `mlsql` and `mlsql` have the permission to read/write `/tmp/__mlsql__`.
         |
         |You can get code example by:
         |
         |```
         |load modelExample.`PythonAlg` as output;
         |```
         |
         |Actually, this doc is also can be get by this command.
         |
         |If you wanna know what params the PythonAlg have, please use the command following:
         |
         |```
         |load modelParam.`PythonAlg` as output;
         |```
         |
     """.stripMargin)
  }
}
