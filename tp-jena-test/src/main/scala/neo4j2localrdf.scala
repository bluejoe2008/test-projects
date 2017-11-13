package honglou

import java.io.File
import org.apache.jena.query.ReadWrite
import org.apache.jena.rdf.model.Model
import org.apache.jena.tdb.TDBFactory
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.JavaConversions._

object neo4j2localrdf {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("bad arguments: neo4j-path, tdb-dir required");
    }
    else {
      val neo4jPath = args(0);
      val tdbPath = args(1);

      new File(tdbPath).mkdirs();

      println(s"neo4j-dir: $neo4jPath, tdb-dir: $tdbPath");

      val graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jPath));
      val dataset = TDBFactory.createDataset(tdbPath);
      dataset.begin(ReadWrite.WRITE);

      val tx = graphDb.beginTx();

      val model = dataset.getDefaultModel();
      var cn = 0;
      graphDb.getAllNodes.foreach { node =>
        insertNode(model, node);
        cn += 1;
      }

      var cl = 0;
      graphDb.getAllRelationships.foreach { rel =>
        insertEdge(model, rel);
        cl += 1;
      }

      println(s"writen $cn nodes, $cl links");

      tx.success();
      model.commit();
      model.close();
      dataset.commit();
      dataset.close();
    }
  }

  def insertNode(model: Model, node: Node): Unit = {
    val nid = node.getId;
    val res = model.createResource(s"http://honglou/resource/$nid");
    node.getAllProperties.foreach { en =>
      val pre = model.createProperty(en._1);
      val obj = model.createTypedLiteral(en._2);

      println(s"writen $res, $pre, $obj");
      model.add(model.createStatement(res, pre, obj));
    }
  }

  def insertEdge(model: Model, rel: Relationship): Unit = {
    val nid = rel.getStartNode.getId;
    val eid = rel.getEndNode.getId;
    val res = model.createResource(s"http://honglou/resource/$nid");
    val obj = model.createResource(s"http://honglou/resource/$eid");
    val pre = model.createProperty(rel.getType.name());

    println(s"writen $res, $pre, $obj");
    model.add(model.createStatement(res, pre, obj));
  }
}