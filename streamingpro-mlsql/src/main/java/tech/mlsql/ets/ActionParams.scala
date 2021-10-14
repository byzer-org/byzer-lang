package tech.mlsql.ets

import java.util.UUID

case class ActionParams(sql: String,
                        owner: String = "admin",
                        jobName: String = UUID.randomUUID().toString,
                        fetchType: String = "take",
                        includeSchema: Boolean = true,
                        options: Map[String, String] = Map())

case class OtherActionParams(timeout: Long = -1,
                             silence: Boolean = false,
                             sessionPerUser: Boolean = true,
                             sessionPerRequest: Boolean = false,
                             async: Boolean = false,
                             callback: String = "",
                             maxRetries: Int = -1,
                             skipInclude: Boolean = false,
                             skipAuth: Boolean = true,
                             skipGrammarValidate: Boolean = false,
                             enableQueryWithIndexer: Boolean = false,
                             executeMode: String = "query",
                             outputSize: Int = 5000
                            )
