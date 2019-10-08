package org.neo4j.kernel.impl

/**
  * Created by bluejoe on 2019/10/7.
  */
object CustomPropertyNodeStoreHolder {
  val _propertyNodeStore = new InMemoryPropertiesStore();
  _propertyNodeStore.init();

  def get = _propertyNodeStore;
}
