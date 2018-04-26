import java.io.BufferedReader
import java.io.File
import java.io.FileFilter
import java.io.FileInputStream
import java.io.InputStreamReader
import java.lang.reflect.Type
import java.util.HashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.unsafe.batchinsert.BatchInserters

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken

/**
 * this is a local program
 * spark is not useful because neo4j works on a single machine
 */
object LocalJsonParts2Neo4jBatchInsert {
	val logger = Logger.getLogger(this.getClass);
	case class GraphOpConsumerContext(inserter: BatchInserter, nodesMap: collection.mutable.Map[String, Long]) {
	}

	case class GraphOpProducerContext(gsonBuilder: Gson, arrayType: Type, mapType: Type, createdNodes: collection.mutable.Map[String, Any]) {
	}

	trait GraphOp {
		def op(ctx: GraphOpConsumerContext) {}
	}

	case class NamedBuckets(name: String, buckets: ArrayBuffer[Array[GraphOp]]) {
	}

	val BUCKET_SIZE = 10000;
	var threads = 4;

	def startGrpahOpsProducerThreadsFromFiles(filePath: String, bucketMap: NamedBuckets)(produceGraphOps: (String) ⇒ Array[GraphOp]): ExecutorService = {
		//list N files
		val ss = new File(filePath).listFiles(new FileFilter() {
			override def accept(path: File) = { !path.getName.startsWith("_"); }
		});

		val fixedThreadPool = Executors.newFixedThreadPool(threads);
		//create N threads
		for (s ← ss) {
			fixedThreadPool.execute(new Runnable() {
				override def run() {
					val in = new FileInputStream(s);
					val fileName = s.getName;
					logger.info(s"open files: $fileName");

					val reader = new BufferedReader(new InputStreamReader(in));
					var line = "";
					val bucket = ArrayBuffer[GraphOp]();
					do {
						//read a line
						line = reader.readLine();
						if (line != null)
							bucket ++= (produceGraphOps(line));
						//bucket is full
						if (bucket.size >= BUCKET_SIZE) {
							insertBucket(bucketMap, bucket.toArray);
							bucket.clear();
						}
					} while (line != null);

					//remaining ops in bucket
					if (!bucket.isEmpty)
						insertBucket(bucketMap, bucket.toArray);
				}
			});
		}
		fixedThreadPool;
	}

	def startGraphOpsConsumerThread(bucketMap: NamedBuckets, ctx: GraphOpConsumerContext, loopCondition: ⇒ Boolean, locks: CountDownLatch*): CountDownLatch = {
		val counter = new CountDownLatch(1);

		new Thread("consumer-thread-" + bucketMap.name) {
			override def run() = {
				val bucketMapName = bucketMap.name;
				if (!locks.isEmpty)
					logger.info(s"******thread $bucketMapName: waiting for unlocked $locks******");
				locks.foreach { _.await(); }

				logger.info(s"******consuming $bucketMapName******");
				var count = 0;

				while (loopCondition) {
					val v =
						bucketMap.buckets.synchronized {
							bucketMap.buckets.headOption;
						}

					//no buckets
					if (v.isEmpty) {
						Thread.sleep(1);
					}
					//consume first bucket
					else {
						v.get.foreach { x ⇒ x.op(ctx) }

						//consumed, drop it
						bucketMap.buckets.synchronized {
							bucketMap.buckets.remove(0);
						}

						val vsize = v.get.size;
						count += 1;
						val bsize = bucketMap.buckets.size;
						logger.info(s"******consumed bucket-$count: (size=$vsize), remaining $bsize buckets******");
					}
				}

				logger.info(s"******consumed $bucketMapName******");
				counter.countDown();
			}
		}.start();
		counter;
	}

	def startNodesGrpahOpsProducerThreads(filePath: String, bucketMap: NamedBuckets, producerContext: GraphOpProducerContext) = {
		startGrpahOpsProducerThreadsFromFiles(filePath, bucketMap)((x: String) ⇒ {
			//a Map json to a node
			val map: java.util.Map[String, Object] = producerContext.gsonBuilder.fromJson(x, producerContext.mapType).asInstanceOf[java.util.Map[String, Object]];
			val map2 = new HashMap[String, Object](map);
			map2.remove("__type__");
			val uri = map.get("__uri__").asInstanceOf[String];
			producerContext.createdNodes.synchronized {
				producerContext.createdNodes += (uri -> "");
			}
			val typeName = map.get("__type__").asInstanceOf[String];

			//return a GraphOp
			Array(new GraphOp() {
				override def op(ctx: GraphOpConsumerContext) {
					val nodeId =
						ctx.inserter.createNode(map2,
							Label.label(typeName));

					ctx.nodesMap += (uri -> nodeId);
				}
			});
		});
	}

	def startLinksGrpahOpsProducerThreads(filePath: String, bucketMap: NamedBuckets, producerContext: GraphOpProducerContext) = {
		startGrpahOpsProducerThreadsFromFiles(filePath, bucketMap)((x: String) ⇒ {
			//an array json to a relationship
			val array: Array[String] = producerContext.gsonBuilder.fromJson(x, producerContext.arrayType);
			val srcNodeUri = array(0);
			val propName = array(1);
			if (producerContext.createdNodes.contains(srcNodeUri)) {
				val dstNodeUri = array(2);
				//yes, it is a link
				if (producerContext.createdNodes.contains(dstNodeUri)) {
					Array(new GraphOp() {
						override def op(ctx: GraphOpConsumerContext) {
							ctx.inserter.createRelationship(ctx.nodesMap(srcNodeUri), ctx.nodesMap(dstNodeUri), RelationshipType.withName(propName), new java.util.HashMap());
						}
					});
				}
				//external link?
				else {
					Array(new GraphOp() {
						override def op(ctx: GraphOpConsumerContext) {
							ctx.inserter.setNodeProperty(ctx.nodesMap(srcNodeUri), propName, dstNodeUri);
						}
					});

				}
			}
			else {
				Array();
			}
		});
	}

	def insertBucket(bucketMap: NamedBuckets, bucket: Array[GraphOp]) = {
		bucketMap.buckets.synchronized {
			bucketMap.buckets.append(bucket);
		}
	}

	def main(args: Array[String]) {
		if (args.length == 4) {
			val nodesFilePath = args(0);
			val linksFilePath = args(1);
			val neo4jDbPath = args(2);
			threads = java.lang.Integer.parseInt(args(3));
			logger.info(s"nodes file: $nodesFilePath, links file: $linksFilePath, neo4j db: $neo4jDbPath, threads: $threads");
			val neo4jDbFile = new File(neo4jDbPath);
			FileUtils.deleteDirectory(neo4jDbFile);

			val producerContext = GraphOpProducerContext(new GsonBuilder().create(),
				new TypeToken[Array[String]]() {}.getType(),
				new TypeToken[java.util.Map[String, Object]]() {}.getType(),
				collection.mutable.Map[String, Any]());

			val inserter = BatchInserters.inserter(neo4jDbFile);
			val nodeOpBucketMap = NamedBuckets("nodes", ArrayBuffer[Array[GraphOp]]());
			val linkOpBucketMap = NamedBuckets("links", ArrayBuffer[Array[GraphOp]]());
			val ctx = GraphOpConsumerContext(inserter, collection.mutable.Map[String, Long]());

			val lock1 = startNodesGrpahOpsProducerThreads(nodesFilePath, nodeOpBucketMap, producerContext);
			val lock2 = startGraphOpsConsumerThread(nodeOpBucketMap, ctx, { !lock1.isTerminated() });
			lock1.awaitTermination(java.lang.Long.MAX_VALUE, TimeUnit.DAYS);
			val lock3 = startLinksGrpahOpsProducerThreads(linksFilePath, nodeOpBucketMap, producerContext);
			val lock4 = startGraphOpsConsumerThread(linkOpBucketMap, ctx, { !lock3.isTerminated() }, lock2);

			lock3.awaitTermination(java.lang.Long.MAX_VALUE, TimeUnit.DAYS);
			lock4.await();

			inserter.shutdown();
			println("******finished******");
		}
		else {
			println("******wrong number of args******");
		}
	}
}