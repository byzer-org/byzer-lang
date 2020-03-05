package streaming.dsl.mmlib.algs

case class ScriptUDFCacheKey(
                              originalCode: String,
                              wrappedCode: String,
                              className: String,
                              udfType: String,
                              methodName: String,
                              dataType: String,
                              lang: String)
