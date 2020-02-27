package tech.mlsql.version

/**
  * 2019-09-11 WilliamZhu(allwefantasy@gmail.com)
  */
trait VersionCompatibility {
  def supportedVersions: Seq[String]
}
