import java.io.{File, FileReader, PrintWriter}
import java.util.Properties

import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

import scala.collection.JavaConversions._

/**
  * Created by bluejoe on 2017/11/30.
  */
object GenerateJsFile {
  def main(args: Array[String]) {
    val props: Properties = new Properties
    props.load(new FileReader(new File("/usr/local/apache-tomcat-7.0.70/webapps/graphviz/WEB-INF/conf.properties")));
    generateJs(props, new PrintWriter(System.out));
  }

  def generateJs(props: Properties, out: PrintWriter): Unit = {
    val file = new File(props.getProperty("target-json-file"));
    if (!file.getParentFile.exists())
      throw new Exception(s"invalid parent dir of json file path: $file");

    val driver = GraphDatabase.driver(props.getProperty("bolt-url"),
      AuthTokens.basic(props.getProperty("bolt-user"),
        props.getProperty("bolt-passwd")));
    val baseUrl = props.getProperty("photo-url-prefix");
    val session = driver.session();

    val writer = new PrintWriter(file, "gb2312");
    writer.println("var nodes = [");

    session.run("MATCH (n) RETURN n").foreach { result =>
      val node = result.get("n").asNode();
      val id = node.id();
      val label = node.get("name").asString();
      val map = node.asMap();
      val jsonMapString = new StringBuffer();
      jsonMapString.append("{");
      Array[String]("community", "pagerank", "degree", "betweenness", "closeness", "photo").foreach { key =>
        if (map.containsKey(key)) {
          val value = map(key);
          val valueString = if (value.isInstanceOf[String]) {
            s"'$value'"
          } else {
            value.toString
          };
          jsonMapString.append(s"$key: $valueString, ");
        }
      }

      jsonMapString.append("label: [");
      node.labels().foreach { label =>
        jsonMapString.append(s"'$label',");
      }
      jsonMapString.append("], ");

      jsonMapString.append("}");

      val it = node.labels().iterator();
      val groupString = if (it.hasNext) {
        val label0 = it.next();
        s"group: '${label0}', "
      } else {
        ""
      };

      writer.println(s"{id:$id, label:'$label', ${groupString}_node_object: {properties: $jsonMapString}},");
    }

    writer.println("];");
    writer.println("var edges = [");
    session.run("MATCH p=()-->() RETURN p").foreach { result =>
      val rel = result.get("p").asPath().relationships().iterator().next();

      val from = rel.startNodeId();
      val to = rel.endNodeId();
      val id = rel.id();
      val label = rel.`type`();
      writer.println(s"{id: $id, label: '$label', from:$from, to:$to, hidden: true},");
    }
    writer.println("];");

    writer.print("var focus = [");
    props.getProperty("example-person-name-list").split(";").foreach { name =>
      session.run(s"MATCH (n) WHERE n.name='$name' RETURN n").foreach { result =>
        val node = result.get("n").asNode();
        writer.print(node.id() + ",");
      }
    }
    writer.println("];");

    writer.flush();
    writer.close();
  }
}
