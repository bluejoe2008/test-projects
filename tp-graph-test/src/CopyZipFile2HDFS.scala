import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.net.URI

object CopyZipFile2HDFS {
	def main(args: Array[String]) = {
		val fn = args(0);
		val hfn = args(1);
		println(s"open file: $fn, save file: $hfn");
		val in = new FileInputStream(new File(fn));
		val conf = new Configuration();
		val fs = FileSystem.get(URI.create(hfn), conf);
		val out = fs.create(new Path(URI.create(hfn)));
		val ungzip = new GZIPInputStream(in);
		var buffer = new Array[Byte](102400);

		var n = 0;
		while (n >= 0) {
			n = ungzip.read(buffer);
			if (n >= 0)
				out.write(buffer, 0, n);
		}

		out.close();
		in.close;
	}
}