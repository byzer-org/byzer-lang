package streaming.dsl.mmlib.algs.meta.graphx

import org.apache.spark.graphx._

/**
  * Created by allwefantasy on 9/8/2018.
  */
case class VeterxAndGroup(vertexId: VertexId, group: VertexId)

case class GroupVeterxs(group: VertexId, vertexIds: Seq[VertexId])
