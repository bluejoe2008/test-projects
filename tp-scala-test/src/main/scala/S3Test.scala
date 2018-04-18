/**
  * Created by bluejoe on 2018/4/18.
  */

import java.io.ByteArrayInputStream
import java.util.List

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{Bucket, ObjectMetadata}
import com.amazonaws.util.{IOUtils, StringUtils}
import com.amazonaws.{ClientConfiguration, Protocol}

import scala.collection.JavaConversions._;

object S3Test {
  def main(args: Array[String]) {
    val accessKey = "APOQIZJAS0JV4GTN0AFF";
    val secretKey = "123";
    val credentials = new BasicAWSCredentials(accessKey, secretKey);

    val clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.HTTP);

    val conn = new AmazonS3Client(credentials, clientConfig);
    conn.setEndpoint("10.0.83.41");
    val buckets: List[Bucket] = conn.listBuckets();
    buckets.foreach((bucket: Bucket) => {
      println(bucket.getName() + "\t" +
        StringUtils.fromDate(bucket.getCreationDate()));
    });

    val data = "Hello World!";
    val key = "123456";
    val input = new ByteArrayInputStream(data.getBytes());
    val md = new ObjectMetadata();

    md.setContentLength(data.getBytes("utf-8").length);
    md.addUserMetadata("prop1", "value1");
    val bucket: String = "Aaa";
    conn.putObject(bucket, key, input, md);

    val o = conn.getObject(bucket, key);
    val content = IOUtils.toString(o.getObjectContent());
    println(content);
    println(o.getObjectMetadata().getUserMetadata);
  }
}
