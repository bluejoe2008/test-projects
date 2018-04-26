import java.io.StringReader
import java.net.URLDecoder
import java.util.regex.Pattern

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.google.gson.GsonBuilder
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime
import com.hp.hpl.jena.rdf.model.ModelFactory

object N3Triples2GraphJson {
	def normalizeURL(src: String) = {
		val regex = "<https?://.*?>";
		val matcher = Pattern.compile(regex).matcher(src);
		val sb = new StringBuffer();
		while (matcher.find()) {
			val uri = matcher.group();
			val i = uri.indexOf("/", 8);
			val uri3 = if (i <= 0) {
				URLDecoder.decode(uri, "utf-8")
			}
			else {
				var uri1 = uri.substring(0, i);
				var uri2 = uri.substring(i + 1, uri.length() - 1);
				URLDecoder.decode(uri1, "utf-8") +
					"/" + uri2.trim()
					.replaceAll("%(.)$", "%25$1")
					.replace("[", "%5B")
					.replace("]", "%5D")
					.replace("{", "%7B")
					.replace("}", "%7C")
					.replace("|", "%49") + ">";
			}

			matcher.appendReplacement(sb, uri3.replace("$", "\\$"));
		}
		matcher.appendTail(sb);
		sb.toString()
	}

	def uri2PropertyName(uri: String) = {
		val i1 = uri.lastIndexOf("#");
		val i2 = uri.lastIndexOf("/");
		uri.substring(Math.max(i1, i2) + 1);
	}

	def resource2JSON(x: Tuple2[String, Iterable[Tuple3[String, String, Any]]]): String = {
		val map = collection.mutable.Map[String, Any]("__uri__" -> x._1);
		//iterates the list
		x._2.foreach { t ⇒
			map += (t._2 -> t._3);
		};
		new GsonBuilder().create().toJson(JavaConversions.mapAsJavaMap(map));
	}

	def link2JSON(t: Tuple3[String, String, String]): String = {
		new GsonBuilder().create().toJson(Array(t._1, t._2, t._3));
	}

	def normalizeValue(value: Object) =
		{
			val v2 =
				value match {
					case x: XSDDateTime ⇒ x.asCalendar().getTimeInMillis;
					case _ ⇒ value;
				}

			v2;
		}

	def extractProperties(line: String) = {
		val source = normalizeURL(line)
		val model = ModelFactory.createDefaultModel();
		model.read(new StringReader(source), null, "N3");

		val ab = new ArrayBuffer[(String, String, Any)]();
		val sts = model.listStatements();
		if (sts.hasNext()) {
			val st = sts.nextStatement();
			val s = st.getSubject;
			val p = st.getPredicate;
			val o = st.getObject;

			val subjectUri = s.getURI;
			val predicateUri = p.getURI;

			//object is an rdf link
			if (o.isResource()) {
				val objectUri = o.asResource().getURI;
				(predicateUri, objectUri) match {
					case (_, "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property") ⇒ ;
					case (_, "http://www.w3.org/2000/01/rdf-schema#Class") ⇒ ;
					case ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", _) ⇒ {
						val t = (subjectUri, "__type__", uri2PropertyName(o.asResource().getURI));
						ab += t;
					}
					case _ ⇒ {
						val t = (subjectUri, uri2PropertyName(p.getURI), o.asResource().getURI);
						ab += t;
					}
				}
			}
			//object is a value
			else {
				val pv = try { normalizeValue(o.asLiteral().getValue); } catch { case _: Throwable ⇒ o.asLiteral().toString(); };
				val t = (subjectUri, uri2PropertyName(predicateUri), pv);
				ab += t;
			}
		}
		model.close()
		ab.toIterable
	}

	def extractLinks(line: String) = {
		val source = normalizeURL(line)
		val ab = new ArrayBuffer[(String, String, String)]();
		val model = ModelFactory.createDefaultModel();
		model.read(new StringReader(source), null, "N3");
		val sts = model.listStatements();
		if (sts.hasNext()) {
			val st = sts.nextStatement();
			val s = st.getSubject;
			val p = st.getPredicate;
			val o = st.getObject;

			val subjectUri = s.getURI;
			val predicateUri = p.getURI;

			//object is an rdf link
			if (o.isResource()) {
				val objectUri = o.asResource().getURI;
				(predicateUri, objectUri) match {
					case (_, "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property") ⇒ ;
					case (_, "http://www.w3.org/2000/01/rdf-schema#Class") ⇒ ;
					case ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", _) ⇒ ;
					case _ ⇒ {
						val t = (subjectUri, uri2PropertyName(p.getURI), o.asResource().getURI);
						ab += t;
					}
				}
			}
		}
		model.close()
		ab.toIterable
	}

	def main(args: Array[String]) = {
		val conf = new SparkConf()
		if (args.length == 2) {
			val master = args(0);
			val srcFile = args(1);
			println(s"master: $master, source file: $srcFile");
			conf.setAppName(this.getClass.getName).setMaster(master);
			val sc = new SparkContext(conf)
			val line = sc.textFile(srcFile)
			line.flatMap { extractProperties }.groupBy(_._1).map(resource2JSON).saveAsTextFile("hdfs://vm122:9000/nodes");
			println("extractProperties finished!");
			line.flatMap { extractLinks }.distinct().map(link2JSON).saveAsTextFile("hdfs://vm122:9000/links");
			println("extractLinks finished!");
		}
		else {
			println("wrong arguments!");
		}
	}
}