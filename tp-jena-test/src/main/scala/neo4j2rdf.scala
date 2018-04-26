package honglou

import java.io.PrintWriter

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.rdfconnection.RDFConnectionFactory
import org.apache.jena.vocabulary.RDF
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.types.{Node, Relationship}

import scala.collection.JavaConversions._

class neo4j2rdf {
  val preds = Map[String, String]("name" -> "http://www.w3.org/2000/01/rdf-schema#label",
    "photo" -> "http://dbpedia.org/ontology/thumbnail");

  def sync(neo4jBoltUrl: String,
           user: String,
           password: String,
           sparqlUpdateUrl: String,
           photoUrlBase: String,
           out: PrintWriter) = {
    val driver = GraphDatabase.driver(neo4jBoltUrl, AuthTokens.basic(user, password));

    out.println(s"neo4j-url: $neo4jBoltUrl, sparql-url: $sparqlUpdateUrl");
    val session = driver.session();

    val model = ModelFactory.createDefaultModel();
    var cn = 0;
    session.run("MATCH (n) RETURN n").foreach { result =>
      val node = result.get("n").asNode();
      insertNode(model, node, photoUrlBase, out);
      cn += 1;
    }

    var cl = 0;
    session.run("MATCH p=()-->() RETURN p").foreach { result =>
      val rel = result.get("p").asPath().relationships().iterator().next();
      insertEdge(model, rel, out);
      cl += 1;
    }

    driver.close();

    out.println(s"writing $cn nodes, $cl links");
    val conn = RDFConnectionFactory.connect(s"$sparqlUpdateUrl/query", s"$sparqlUpdateUrl/update", s"$sparqlUpdateUrl/data");
    //clear
    conn.delete();
    conn.load(model);
    conn.close();

    out.println(s"writen $cn nodes, $cl links");
  }

  def insertNode(model: Model, node: Node, photoUrlBase: String, out: PrintWriter): Unit = {
    val nid = node.id();
    val res = model.createResource(s"http://honglou/resource/$nid");

    node.labels().foreach { label =>
      model.add(model.createStatement(res, RDF.`type`,
        model.createResource(label)));
    }

    node.asMap().foreach { en =>
      val relname = en._1;
      val isPhoto = relname.equals("photo");
      val pre = model.createProperty(preds.getOrElse(relname, relname));
      val obj = if (isPhoto) {
        model.createResource(photoUrlBase + en._2);
      } else {
        model.createTypedLiteral(en._2);
      }

      out.println(s"writen $res, $pre, $obj");
      model.add(model.createStatement(res, pre, obj));
    }
  }

  def insertEdge(model: Model, rel: Relationship, out: PrintWriter): Unit = {
    val nid = rel.startNodeId();
    val eid = rel.endNodeId();
    val res = model.createResource(s"http://honglou/resource/$nid");
    val obj = model.createResource(s"http://honglou/resource/$eid");
    val relname = rel.`type`();
    val pre = model.createProperty(relname);

    out.println(s"writen $res, $pre, $obj");
    model.add(model.createStatement(res, pre, obj));
  }
}