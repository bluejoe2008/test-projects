package org.neo4j.kernel.impl

import org.neo4j.cypher.internal.runtime.interpreted.NodeFieldPredicate

/**
  * Created by bluejoe on 2019/10/7.
  */
object CustomPropertyNodeStoreHolder {
  val _propertyNodeStore = new LoggingPropertiesStore(new InMemoryPropertiesStore());
  _propertyNodeStore.init();

  def get = _propertyNodeStore;
}

class LoggingPropertiesStore(source: CustomPropertyNodeStore) extends CustomPropertyNodeStore {
  override def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    println(s"deleteNodes: $docsToBeDeleted")
    source.deleteNodes(docsToBeDeleted)
  }

  override def init(): Unit = {
    println(s"init()")
    source.init()
  }

  override def addNodes(docsToAdded: Iterable[CustomPropertyNode]): Unit = {
    println(s"deleteNodes:$docsToAdded")
    source.addNodes(docsToAdded)
  }

  override def filterNodes(expr: NodeFieldPredicate): Iterable[CustomPropertyNode] = {
    println(s"filterNodes: $expr")
    source.filterNodes(expr)
  }

  override def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]): Unit = {
    println(s"docsToUpdated: $docsToUpdated")
    source.updateNodes(docsToUpdated)
  }
}
