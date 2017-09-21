import scala.tools.nsc.interpreter._
import scala.tools.nsc.Settings
import scala.sys.Prop
import scala.reflect.runtime.Settings

object ReplTest {
	def foo() = new Object {
		override def toString() = "!@#$%^";
	}

	def main(args: Array[String]): Unit = {
		val repl = new ILoop {
			override def createInterpreter() = {
				super.createInterpreter();
				intp.bind("author", "String", "bluejoe2008@gmail.com")
			}
			override def printWelcome(): Unit = {
				Console println "yyyyy";
			}
			override def prompt = ">>>";
		}
		
		val settings = new Settings
		settings.Yreplsync.value = true
		//use when launching normally outside SBT
		settings.usejavacp.value = true
		System.setProperty("scala.repl.prompt", "sci-ql>");
		System.setProperty("scala.repl.welcome", """welcome to sci-ql command line interface! copyrightc BigScientificDataMgmt
^(*￣(oo)￣)^		  
		  """);
		repl.process(settings)
	}
}
