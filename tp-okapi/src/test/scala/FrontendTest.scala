import java.util.regex.MatchResult

import org.junit.{Before, Test}
import org.opencypher.v9_0.ast.semantics.SemanticErrorDef
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.util.symbols.CTString
import org.opencypher.v9_0.util.{CypherException, InputPosition}
import org.parboiled.matchers.Matcher

class FrontendTest {
  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  private val pipeline = Parsing andThen SemanticAnalysis(warn = true)

  @Test
  def test1(): Unit = {
    val query = "match (n)-[dad]->(m) where m.age>35 return n.name"
    val startState = InitialState(query, None, NoPlannerName, Map())

    val context = new ErrorCollectingContext()
    val r = pipeline.transform(startState, context)
    println(r);
  }
}

object NoPlannerName extends PlannerName {
  override def name = "no planner"

  override def toTextOutput = "no planner"

  override def version = "no version"
}

class ErrorCollectingContext extends BaseContext {

  var errors: Seq[SemanticErrorDef] = Seq.empty

  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING

  override def notificationLogger: devNullLogger.type = devNullLogger

  override def monitors: Monitors = ???

  override def errorHandler: Seq[SemanticErrorDef] => Unit = (errs: Seq[SemanticErrorDef]) =>
    errors = errs

  override def exceptionCreator: (String, InputPosition) => CypherException = null
}