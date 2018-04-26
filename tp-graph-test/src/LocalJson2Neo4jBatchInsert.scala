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

/**
 * this is a local program
 * spark is not useful because neo4j works on a single machine
 */
object LocalJson2Neo4jBatchInsert {
	def processLines(filePath: String, processLine: (String) ⇒ Unit) {
		val t1 = System.currentTimeMillis();
		//all text files in a HDFS directory
		val lines = Source.fromFile(filePath).getLines();
		for (line ← lines) {
			processLine(line);
		}
		val t2 = System.currentTimeMillis();
		println("time: " + (t2 - t1) + " ms");
	}

	def insert(nodesFilePath: String, linksFilePath: String, neo4jDbPath: String) = {
		val neo4jDbFile = new File(neo4jDbPath);
		FileUtils.deleteDirectory(neo4jDbFile);
		val inserter = BatchInserters.inserter(neo4jDbFile);
		val mapType = new TypeToken[java.util.Map[String, Object]]() {}.getType();
		val arrayType = new TypeToken[Array[String]]() {}.getType();
		val gsonBuilder = new GsonBuilder().create();
		val nodesMap = collection.mutable.Map[String, Long]();
		var count = 0;
		processLines(nodesFilePath, (x: String) ⇒ {
			//a Map json to a node
			val map: java.util.Map[String, Object] = gsonBuilder.fromJson(x, mapType).asInstanceOf[java.util.Map[String, Object]];
			val map2 = new HashMap[String, Object](map);
			map2.remove("__type__");
			val nodeId = inserter.createNode(map,
				Label.label(map.get("__type__").asInstanceOf[String]));

			nodesMap += (map.get("__uri__").asInstanceOf[String] -> nodeId);
			count = count + 1;
			if (count % 10000 == 0)
				println(s"*******$count*******");
		});

		count = 0;
		var count1 = 0;
		var count2 = 0;
		processLines(linksFilePath, (x: String) ⇒ {
			//an array json to a relationship
			val array: Array[String] = gsonBuilder.fromJson(x, arrayType);
			val srcNodeUri = array(0);
			val optSrc = nodesMap.get(srcNodeUri);
			if (!optSrc.isEmpty) {
				val dstNodeUri = array(2);
				//yes, it is a link
				val optDst = nodesMap.get(dstNodeUri);
				if (!optDst.isEmpty) {
					inserter.createRelationship(optSrc.get, optDst.get, RelationshipType.withName(array(1)), new java.util.HashMap());
					count1 = count1 + 1;
				}
				//external link?
				else {
					inserter.setNodeProperty(optSrc.get, array(1), dstNodeUri);
					count2 = count2 + 1;
				}
			}

			count = count + 1;
			if (count % 10000 == 0)
				println(s"*******total=$count(internal:$count1, external:$count2)*******");
		});

		inserter.shutdown();
		println("******finished******");
	}

	def main(args: Array[String]) {
		if (args.length == 3) {
			val nodesFilePath = args(0);
			val linksFilePath = args(1);
			var neo4jDbPath = args(2);
			println(s"nodes file: $nodesFilePath, links file: $linksFilePath, neo4j db: $neo4jDbPath");
			insert(nodesFilePath, linksFilePath, neo4jDbPath);
		}
		else {
			println("******wrong number of args******");
		}
	}
}