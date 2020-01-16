package tech.mlsql.plugins.app.pythoncontroller.quill_model

/**
 * 16/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
case class ScriptFile(id: Int,
                      name: String,
                      hasCaret: Int,
                      icon: String,
                      label: String,
                      parentId: Int,
                      isDir: Int,
                      content: String,
                      isExpanded: Int
                     )

case class ScriptUserRw(id: Int,
                        scriptFileId: Int,
                        mlsqlUserId: Int,
                        isOwner: Int,
                        readable: Int,
                        writable: Int,
                        isDelete: Int
                       )

case class MlsqlUser(id: Int,
                     name: String,
                     password: String,
                     backendTags: String,
                     role: String,
                     status: String
                    )

case class AccessToken(id: Int, name: String, mlsqlUserId: Int, createAt: Long)
