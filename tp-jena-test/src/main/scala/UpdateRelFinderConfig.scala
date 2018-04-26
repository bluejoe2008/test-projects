package honglou

import java.io._

import org.apache.commons.io.IOUtils
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

/**
  * Created by bluejoe on 2017/11/29.
  */
class UpdateRelFinderConfig {
  def updateExampleFile(neo4jBoltUrl: String, user: String, password: String,
                        exampleFileDir: File, personNames: Array[String],
                        out: PrintWriter): Unit = {
    if (!exampleFileDir.exists())
      throw new Exception(s"invalid directory: $exampleFileDir");

    val driver = GraphDatabase.driver(neo4jBoltUrl, AuthTokens.basic(user, password));
    val tf = new File(exampleFileDir, "examples.xml.template");
    if (!tf.exists())
      throw new Exception(s"invalid directory: $tf");

    val template = IOUtils.toString(new FileInputStream(tf), "utf-8");
    val session = driver.session();
    val replacement = new StringBuffer();
    for (i <- 0 to personNames.length - 1) {
      val name = personNames(i);
      val nodeid = session.run(s"MATCH (n:person) WHERE n.name='$name' RETURN id(n) as id").
        next().get("id");
      replacement.append(
        s"""
      <object>
        <label>${name}</label>
        <uri>http://honglou/resource/${nodeid}</uri>
      </object>
      """);
      out.println(s"added example node: $name, nodeid: $nodeid");
    }

    driver.close();
    val ef = new File(exampleFileDir, "examples.xml");
    val writer = new FileWriter(ef);
    writer.write(template.replace("!!!HONGLOU_EXAMPLE_OBJECTS!!!", replacement.toString));
    writer.close();
    out.println(s"example file created: $ef");
  }
}
