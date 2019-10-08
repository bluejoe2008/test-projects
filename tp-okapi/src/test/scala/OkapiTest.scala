import org.junit.Test

import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.v9_0.frontend.phases.BaseContext

class OkapiTest {
  @Test
  def test1(): Unit = {
    implicit val context:BaseContext = CypherParser.defaultContext;
    val st = CypherParser.process("match (n)-[dad]->(m) where m.age>35 return n.name")._1
    println(st);
    //st.rewrite()
  }
}