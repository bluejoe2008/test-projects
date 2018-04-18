import java.io.File

import com.ceph.rados.Rados
import com.ceph.rados.exceptions.RadosException

object CephTest {
  def main(args: Array[String]) {
    try {
      val cluster = new Rados("admin");
      System.out.println("Created cluster handle.");

      val f = new File("./ceph.conf");
      cluster.confReadFile(f);
      System.out.println("Read the configuration file.");

      cluster.connect();
      System.out.println("Connected to the cluster.");

    } catch {
      case e: RadosException
      => System.out.println(e.getMessage() + ": " + e.getReturnValue());
    }
  }
}