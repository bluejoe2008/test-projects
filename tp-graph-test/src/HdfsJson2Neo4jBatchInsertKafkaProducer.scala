import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions

/**
 * this is a local program
 * spark is not useful because neo4j works on a single machine
 */
object HdfsJson2Neo4jBatchInsertKafkaProducer {
	val KAFKA_PROPERTIES = new Properties();
	KAFKA_PROPERTIES.put("zookeeper.connect", "vm105:2181,vm106:2181,vm107:2181,vm181:2181,vm182:2181");
	KAFKA_PROPERTIES.put("group.id", "neo4j");
	KAFKA_PROPERTIES.put("zookeeper.session.timeout.ms", "400");
	KAFKA_PROPERTIES.put("zookeeper.sync.time.ms", "200");
	KAFKA_PROPERTIES.put("auto.commit.interval.ms", "1000");
	KAFKA_PROPERTIES.put("bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092");
	KAFKA_PROPERTIES.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	KAFKA_PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	KAFKA_PROPERTIES.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	KAFKA_PROPERTIES.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

	def produceKafkaMessages(topic: String, lines: Iterator[String]): Iterator[Long] = {
		val producer = new KafkaProducer[String, String](KAFKA_PROPERTIES);
		var linesCount = 0;
		while (lines.hasNext) {
			val line = lines.next();
			linesCount = linesCount + 1;
			producer.send(new ProducerRecord[String, String](topic, "" + linesCount, line));
		}
		producer.close();
		Array[Long](linesCount).iterator;
	}

	def main(args: Array[String]): Unit = {
		if (args.length == 2) {
			val nodesFilePath = args(0);
			val linksFilePath = args(1);

			println(s"nodes file: $nodesFilePath, links file: $linksFilePath");
			val conf = new SparkConf()
			conf.setAppName(this.getClass.getName).setMaster("spark://vm122:7077");
			val sc = new SparkContext(conf)
			val producer = new KafkaProducer[String, String](KAFKA_PROPERTIES);

			val nodes = sc.textFile(nodesFilePath)
			val nodesCount = nodes.mapPartitions { produceKafkaMessages("nodes", _) }.sum();
			producer.send(new ProducerRecord[String, String]("nodes", "count", "" + nodesCount));

			val links = sc.textFile(linksFilePath)
			val linksCount = links.mapPartitions { produceKafkaMessages("links", _) }.sum();
			producer.send(new ProducerRecord[String, String]("links", "count", "" + linksCount));

			producer.close();
		}
		else {
			println("******wrong number of args******");
		}
	}
}