import java.io.File
import java.util.HashMap

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConversions.seqAsJavaList

import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.unsafe.batchinsert.BatchInserters

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig

/**
 * this is a local program
 * spark is not useful because neo4j works on a single machine
 */
object HdfsJson2Neo4jBatchInsertKafkaConsumer {
	val mapType = new TypeToken[java.util.Map[String, Object]]() {}.getType();
	val arrayType = new TypeToken[Array[String]]() {}.getType();
	val gsonBuilder = new GsonBuilder().create();
	val nodesMap = collection.mutable.Map[String, Long]();

	def consumeNodes(inserter: BatchInserter) = {
		val consumer = new KafkaConsumer[String, String](HdfsJson2Neo4jBatchInsertKafkaProducer.KAFKA_PROPERTIES);
		consumer.subscribe(List("nodes"));
		var expectedCount: Long = -1;
		var count: Long = 0;
		while (expectedCount == -1 || count <= expectedCount) {
			val records = consumer.poll(1000);
			for (record ← records) {
				val x = record.value();
				if ("count".equals(record.key())) {
					expectedCount = java.lang.Long.parseLong(x);
				}
				else {
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
				}
			}
		}
	}

	def consumeLinks(inserter: BatchInserter) = {
		val consumer = new KafkaConsumer[String, String](HdfsJson2Neo4jBatchInsertKafkaProducer.KAFKA_PROPERTIES);
		consumer.subscribe(List("links"));
		var expectedCount: Long = -1;
		var count: Long = 0;
		var count1 = 0;
		var count2 = 0;
		while (expectedCount == -1 || count <= expectedCount) {
			val records = consumer.poll(100);
			for (record ← records) {
				val x = record.value();
				if ("count".equals(record.key())) {
					expectedCount = java.lang.Long.parseLong(x);
				}
				else {
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
				}
			}
		}
	}

	def main(args: Array[String]) {
		if (args.length == 1) {
			var neo4jDbPath = args(0);
			println(s"neo4j db: $neo4jDbPath");

			val neo4jDbFile = new File(neo4jDbPath);
			FileUtils.deleteDirectory(neo4jDbFile);
			val inserter = BatchInserters.inserter(neo4jDbFile);
			consumeNodes(inserter);
			consumeLinks(inserter);
			inserter.shutdown();
		}
		else {
			println("******wrong number of args******");
		}
	}
}