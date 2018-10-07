package streaming.test.pythonalg.code

case class ScriptCode(modelPath: String, projectPath: String, featureTablePath: String = "/tmp/featureTable")

object ScriptCode {


  val train =
    """
      |load csv.`${projectPath}/wine-quality.csv`
      |where header="true" and inferSchema="true"
      |as data;
      |
      |
      |
      |train data as PythonAlg.`${modelPath}`
      |
      | where pythonScriptPath="${projectPath}"
      | and keepVersion="true"
      | and  enableDataLocal="true"
      | and  dataLocalFormat="csv"
      | ;
    """.stripMargin

  val processing =
    """
      |set feature_fun='''
      |
      |def apply(self,m):
      |    import json
      |    obj = json.loads(m)
      |    features = []
      |    for attribute, value in obj.items():
      |        if attribute != "quality":
      |            features.append(value)
      |    return features
      |''';
      |
      |load script.`feature_fun` as scriptTable;
      |
      |register ScriptUDF.`scriptTable` as featureFun options
      |and lang="python"
      |and dataType="array(double)"
      |;
      |
      |
      | select featureFun(to_json(struct(*))) as features from data  as featureTable;
    """.stripMargin


  val batchPredict =
    """
      | predict data as PythonAlg.`${modelPath}`;
    """.stripMargin

  val apiPredict =
    """
      | register PythonAlg.`${modelPath}` as pj;
    """.stripMargin
}
