package honglou

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.rdfconnection.RDFConnectionFactory
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.types.{Node, Relationship}

import scala.collection.JavaConversions._

object neo4j2rdf {
  val preds = Map[String, String]("姓名" -> "http://www.w3.org/2000/01/rdf-schema#label",
    "名字" -> "http://www.w3.org/2000/01/rdf-schema#label");

  def sync(neo4jBoltUrl: String, user: String, password: String, sparqlUpdateUrl: String): Array[Int] = {
    val driver = GraphDatabase.driver(neo4jBoltUrl, AuthTokens.basic(user, password));

    println(s"neo4j-url: $neo4jBoltUrl, sparql-url: $sparqlUpdateUrl");
    val session = driver.session();

    val model = ModelFactory.createDefaultModel();
    var cn = 0;
    session.run("MATCH (n) RETURN n").foreach { result =>
      val node = result.get("n").asNode();
      insertNode(model, node);
      cn += 1;
    }

    var cl = 0;
    session.run("MATCH p=()-->() RETURN p").foreach { result =>
      val rel = result.get("p").asPath().relationships().iterator().next();
      insertEdge(model, rel);
      cl += 1;
    }

    driver.close();

    println(s"writing $cn nodes, $cl links");
    val conn = RDFConnectionFactory.connect(s"$sparqlUpdateUrl/query", s"$sparqlUpdateUrl/update", s"$sparqlUpdateUrl/data");
    //clear
    conn.delete();
    conn.load(model);
    conn.close();

    Array(cn, cl);
  }

  def main(args: Array[String]) {
    val Array(cn, cl) = sync(args(0), args(1), args(2), args(3));
    println(s"writen $cn nodes, $cl links");
  }

  def insertNode(model: Model, node: Node): Unit = {
    val nid = node.id();
    val res = model.createResource(s"http://honglou/resource/$nid");
    node.asMap().foreach { en =>
      val relname = en._1;
      val pre = model.createProperty(preds.getOrElse(relname, relname));
      val obj = model.createTypedLiteral(en._2);

      println(s"writen $res, $pre, $obj");
      model.add(model.createStatement(res, pre, obj));
    }
  }

  def insertEdge(model: Model, rel: Relationship): Unit = {
    val nid = rel.startNodeId();
    val eid = rel.endNodeId();
    val res = model.createResource(s"http://honglou/resource/$nid");
    val obj = model.createResource(s"http://honglou/resource/$eid");
    val relname = rel.`type`();
    val pre = model.createProperty(relname);

    println(s"writen $res, $pre, $obj");
    model.add(model.createStatement(res, pre, obj));
  }
}