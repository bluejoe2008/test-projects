import java.io.PrintWriter
import java.io.FileWriter
import scala.io.Source
import java.io.File

object SplitDataset {
	def main(args: Array[String]) = {
		val source = Source.fromFile("/Users/bluejoe/datasets/dblp/dblp.rdf").getLines();
		var lines = 0;
		var i = 0;
		var PAGE_SIZE = 10000;
		while (source.hasNext) {
			val sb = new StringBuffer();

			for (i ← 0 to PAGE_SIZE if source.hasNext) {
				sb.append(source.next() + "\r\n");
				lines = lines + 1;
			}

			val fw = new FileWriter(new File("/Users/bluejoe/datasets/dblp/splits/dblp.rdf." + i));
			new PrintWriter(fw).println(sb.toString());
			fw.close();

			i = i + 1;

			println(s"outputed $i files, $lines lines.");
		}
	}
}