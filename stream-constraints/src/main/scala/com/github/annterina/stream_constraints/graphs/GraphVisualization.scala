package com.github.annterina.stream_constraints.graphs

import java.io.File

import guru.nidi.graphviz.engine.{Format, Graphviz}
import scalax.collection.edge.LDiEdge
import scalax.collection.io.dot.{DotAttr, DotAttrStmt, DotEdgeStmt, DotGraph, DotNodeStmt, DotRootGraph, Elem, Id, NodeId, graph2DotExport}
import scalax.collection.mutable.Graph
import scalax.collection.{Graph => ImGraph}

object GraphVisualization {

  private val dotRoot = DotRootGraph(directed = true,
    id = Some(Id("Constraints")),
    attrStmts = List(DotAttrStmt(Elem.node, List(DotAttr(Id("shape"), Id("circle"))))))

  def visualize(graph: Graph[ConstraintNode, LDiEdge]): File = {
    Graphviz.fromString(graph.toDot(dotRoot, edgeTransformer, cNodeTransformer = Some(nodeTransformer),
      iNodeTransformer = Some(nodeTransformer)))
      .scale(2)
      .render(Format.PNG)
      .toFile(new File(s"graphs/constraints.png"))
  }

  private def edgeTransformer(innerEdge: ImGraph[ConstraintNode, LDiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
    val edge = innerEdge.edge
    edge.label match {
      case WindowLabel(window, action) => Some(dotRoot, DotEdgeStmt(NodeId(edge.from.value.name), NodeId(edge.to.value.name),
        List(DotAttr(Id("label"), Id(" " ++ action.toString ++ " within " ++ window.toMillis.toString ++ " ms")),
        DotAttr(Id("color"), Id("#3d5bd1")))))
      case _ => Some(dotRoot, DotEdgeStmt(NodeId(edge.from.value.name), NodeId(edge.to.value.name), Nil))
    }
  }

  private def nodeTransformer(innerNode: ImGraph[ConstraintNode, LDiEdge]#NodeT): Option[(DotGraph, DotNodeStmt)] =
    if (innerNode.value.nodeType == "TERMINAL") {
      Some(dotRoot, DotNodeStmt(NodeId(innerNode.value.name), List(DotAttr(Id("shape"), Id("doublecircle")))))
    } else None
}
