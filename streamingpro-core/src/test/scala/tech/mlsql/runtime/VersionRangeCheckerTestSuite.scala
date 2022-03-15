package tech.mlsql.runtime

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class VersionRangeCheckerTestSuite extends AnyFlatSpec with should.Matchers {

  it should "==" in {
    val eq1 = EqDef.accept("==2.1.0")
    assert(eq1.inScope("2.1.0"))

    // test white space
    val eq2 = EqDef.accept("== 2.1.0-SNAPSHOT")
    assert(eq2.inScope("2.1.0-SNAPSHOT"))

    val eq3 = EqDef.accept("=2.1.0-Beta")
    assert(!eq3.inScope("2.1.0"))
  }

  it should ">" in {
    val gt1 = GtDef.accept(">2.1.0")
    assert(gt1.inScope("2.1.1"))

    val gt2 = GtDef.accept("(2.1.0, )")
    assert(gt2.inScope("2.1.1"))

    val gt3 = GtDef.accept("(2.1.0, )")
    assert(!gt3.inScope("2.1.0"))

    val gt4 = GtDef.accept(">=2.1.0")
    assert(gt4 == null)

    val gt5 = GtDef.accept(">2.11")
    assert(!gt5.inScope("2.2"))
  }

  it should ">=" in {
    val gt1 = GeDef.accept(">=2.1.0")
    assert(gt1.inScope("2.1.1"))

    val gt2 = GeDef.accept("[2.1.0, )")
    assert(gt2.inScope("2.1.0"))

    val gt3 = GeDef.accept("2.1.0")
    assert(gt3.inScope("2.1.1"))

    val gt4 = GeDef.accept(">=2.1.0")
    assert(!gt4.inScope("2.1.0-Dev"))
  }

  it should "<" in {
    val lt1 = LtDef.accept("<2.1.0")
    assert(lt1.inScope("2.1.0-Alpha"))

    val lt2 = LtDef.accept("(, 2.1.0)")
    assert(lt2.inScope("2.0.9"))
  }

  it should "<=" in {
    val le1 = LeDef.accept("<=2.1.0")
    assert(le1.inScope("2.1.0"))

    val le2 = LeDef.accept("(, 2.1.0]")
    assert(le2.inScope("2.1.0-Beta"))
  }

  it should ">v1 and <v2" in {
    val gtLt1 = GtLtDef.accept("(2.0.1, 2.1.5)")
    assert(gtLt1.inScope("2.1.0"))

    val gtLt2 = GtLtDef.accept(">2.0.1, <2.1.5")
    assert(gtLt2.inScope("2.1.0-Beta"))

  }

  it should ">=v1 and <v2" in {
    val geLt1 = GeLtDef.accept("[2.0.1, 2.1.5)")
    assert(!geLt1.inScope("2.0.1-dev"))

    val geLt2 = GeLtDef.accept(">=2.0.1, <2.1.5")
    assert(geLt2.inScope("2.1.0-Beta"))

    val geLt3 = GeLtDef.accept("==2.0.1, <2.1.5")
    assert(geLt3 == null)

    val geLt4 = GeLtDef.accept("[2.0.1-SNAPSHOT, 2.1.5)")
    assert(geLt4.inScope("2.1.5-Beta"))
  }

  it should ">v1 and <=v2" in {
    val gtLe1 = GtLeDef.accept("(2.0.1, 2.1.5]")
    assert(gtLe1.inScope("2.0.2"))

    val gtLe2 = GtLeDef.accept(">2.0.1, <=2.1.5")
    assert(gtLe2.inScope("2.1.0-Beta"))

    val gtLe3 = GtLeDef.accept(">2.0.1, <=2.1.5-Dev")
    assert(gtLe3.inScope("2.1.5-Beta"))
  }

  it should ">=v1 and <=v2" in {
    val geLe1 = GeLeDef.accept("[2.0.1, 2.1.5]")
    assert(geLe1.inScope("2.1.5"))

    val geLe2 = GeLeDef.accept(">=2.0.1, <=2.1.5")
    assert(geLe2.inScope("2.1.0-Beta"))

    val geLe3 = GeLeDef.accept("[2.0.1, <=2.1.5")
    assert(geLe3 == null)
  }

  it should "version checker" in {
    assertThrows[RuntimeException](VersionRangeChecker.isVersionCompatible("[2.1.0, <= 2.2.0", "2.1.5"))
    assert(VersionRangeChecker.isVersionCompatible("[2.1.0, 2.2.0]", "2.1.5"))
    assert(VersionRangeChecker.isVersionCompatible("(2.1.0, 2.2.0-SNAPSHOT]", "2.2.0-SNAPSHOT"))
    assert(VersionRangeChecker.isVersionCompatible("<2.1.0", "2.1.0-Dev"))
    assert(VersionRangeChecker.isVersionCompatible(">=2.1.0", "2.1.3-Beta"))
  }

  it should "composed version checker" in {
    assertThrows[RuntimeException](VersionRangeChecker.isComposedVersionCompatible("[2.1.0,  2.2.0)/<>2.10", "2.1.5", "2.10"))
    assert(!VersionRangeChecker.isComposedVersionCompatible("[2.1.0,  2.2.0)/>2.12", "2.1.5", "2.2"))
    assert(VersionRangeChecker.isComposedVersionCompatible("(2.1.0,  2.2.0)/<3.11", "2.1.5", "3.2.5"))
    assert(VersionRangeChecker.isComposedVersionCompatible("(2.1.0,  2.2.0)", "2.2.0-dev", ""))
  }

}
