import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader

import scala.collection.JavaConversions

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserters

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import java.net.URI
import scala.io.Source
import java.util.HashMap
import java.util.Random

/**
 * this is a local program
 * spark is not useful because neo4j works on a single machine
 */
object Neo4jBatchInsertTest {
	def test(it: Iterator[String]) {
		val neo4jDbPath = "./testdb";
		val neo4jDbFile = new File(neo4jDbPath);
		FileUtils.deleteDirectory(neo4jDbFile);
		val inserter = BatchInserters.inserter(neo4jDbFile);
		val t1 = System.currentTimeMillis();
		var count = 0;
		val nodesMap = collection.mutable.Map[String, Long]();
		val gsonBuilder = new GsonBuilder().create();
		val mapType = new TypeToken[java.util.Map[String, Object]]() {}.getType();
		val arrayType = new TypeToken[Array[String]]() {}.getType();
		for (x ← it) {
			//val x = """{"a":"x","b":"y","__uri__":"http://abc.com","__type__":"Test"}""";
			val map: java.util.Map[String, Object] = gsonBuilder.fromJson(x, mapType).asInstanceOf[java.util.Map[String, Object]];
			val map2 = new HashMap[String, Object](map);
			map2.remove("__type__");
			val nodeId = inserter.createNode(map,
				Label.label(map.get("__type__").asInstanceOf[String]));

			nodesMap += (map.get("__uri__").asInstanceOf[String] -> nodeId);
			count = count + 1;
			if (count % 10000 == 0)
				println(s"*******$count*******");
		}

		val t2 = System.currentTimeMillis();
		inserter.shutdown();
		println("time: " + (t2 - t1) + " ms");
	}

	def main(args: Array[String]) {
		test((0 to 5125904).map { x ⇒ """{"a":"x","b":"y","__uri__":"http://abc.com","__type__":"Test"}""" }.toIterator);
		test(Source.fromFile(new File("/Users/bluejoe/datasets/dblp/nodes.json")).getLines());
	}
}