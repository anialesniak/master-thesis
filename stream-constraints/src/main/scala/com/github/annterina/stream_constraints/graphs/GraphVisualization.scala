package com.github.annterina.stream_constraints.graphs

import java.io.File

import com.github.annterina.stream_constraints.constraints.window.{DropAfter, DropBefore, Swap, WindowAction}
import guru.nidi.graphviz.engine.{Format, Graphviz}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.edge.LDiEdge
import scalax.collection.io.dot._
import scalax.collection.mutable.Graph
import scalax.collection.{Graph => ImGraph}

object GraphVisualization {

  private val root = DotRootGraph(directed = true, id = Some(Id("Constraints")),
    attrStmts = List(DotAttrStmt(Elem.node, List(DotAttr(Id("shape"), Id("circle"))))))

  def visualize(windowGraph: Graph[ConstraintNode, LDiEdge], graph: Graph[ConstraintNode, DiEdge]): File = {
    val prerequisiteConstraints = graph.toDot(root, diEdgeTransformer, cNodeTransformer = Some(nodeTransformer),
      iNodeTransformer = Some(nodeTransformer))
      .replace("digraph Constraints {", "")
      .replace("}", "")

    val windowConstraints = windowGraph.toDot(root, lDiEdgeTransformer)
      .replace("digraph Constraints {", "")
      .replace("}", "")

    Graphviz.fromString(format(prerequisiteConstraints, windowConstraints))
      .scale(2)
      .render(Format.PNG)
      .toFile(new File(s"graphs/constraints.png"))
  }

  private def lDiEdgeTransformer(innerEdge: ImGraph[ConstraintNode, LDiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
    val edge = innerEdge.edge
    val label = edge.label.asInstanceOf[WindowLabel]
    Some(root, DotEdgeStmt(NodeId(edge.from.value.name), NodeId(edge.to.value.name),
      List(arrowType(label.action), DotAttr(Id("color"), Id("#3d5bd1")),
        DotAttr(Id("label"), Id(" " ++ label.window.toMillis.toString ++ " ms")))))
  }

  private def diEdgeTransformer(innerEdge: ImGraph[ConstraintNode, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
    val edge = innerEdge.edge
    Some(root, DotEdgeStmt(NodeId(edge.from.value.name), NodeId(edge.to.value.name), Nil))
  }

  private def nodeTransformer(innerNode: ImGraph[ConstraintNode, DiEdge]#NodeT): Option[(DotGraph, DotNodeStmt)] =
    if (innerNode.value.terminal) {
      Some(root, DotNodeStmt(NodeId(innerNode.value.name), List(DotAttr(Id("shape"), Id("doublecircle")))))
    } else None

  private def arrowType(action: WindowAction): DotAttr = {
    action match {
      case Swap       => DotAttr(Id("arrowhead"), Id("inv"))
      case DropBefore => DotAttr(Id("arrowhead"), Id("invempty"))
      case DropAfter  => DotAttr(Id("arrowhead"), Id("empty"))
    }
  }

  private def format(prerequisites: String, windows: String): String = {
    val prerequisitesEdges = prerequisites
      .replace("digraph Constraints {", "")
      .replace("}", "")

    val windowEdges = windows
      .replace("digraph Constraints {", "")
      .replace("}", "")

    s"digraph Constraints {\n subgraph PrerequisiteConstraints { $prerequisitesEdges } \n\n " +
      s"subgraph WindowConstraints { $windowEdges } \n}\n"
  }
}
