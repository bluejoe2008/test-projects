/**
  * Created by bluejoe on 2017/11/29.
  */
package honglou

import java.io.PrintWriter
import java.util.Properties

import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

class ExecuteProcedures {
  def execute(props: Properties, out: PrintWriter) = {
    val neo4jBoltUrl = props.get("bolt-url").toString;
    val user = props.get("bolt-user").toString;
    val password = props.get("bolt-passwd").toString;

    val driver = GraphDatabase.driver(neo4jBoltUrl, AuthTokens.basic(user, password));
    val session = driver.session();
    var i = 1;
    while (props.containsKey(s"neo4j-procedure-$i")) {
      val cmd = props.getProperty(s"neo4j-procedure-$i");
      session.run(cmd);

      out.println(s"executed: $cmd");
      i += 1;
    }

    out.println(s"executed $i procedures");
  }
}
