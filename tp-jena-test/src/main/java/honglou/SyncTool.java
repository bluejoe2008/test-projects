package honglou;

import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Properties;

/**
 * Created by bluejoe on 2017/11/12.
 */
public abstract class SyncTool {
    public static void main(String[] args) {
        try {
            sync(new File(args[0]), new PrintWriter(System.out, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sync(File file, PrintWriter out) throws Exception {
        Properties props = new Properties();
        if (!file.exists())
            throw new Exception("invalid properties file path: " + file.getAbsolutePath());

        props.load(new FileReader(file));

        new neo4j2rdf().sync(props.get("bolt-url").toString(),
                props.get("bolt-user").toString(),
                props.get("bolt-passwd").toString(),
                props.get("sparql-update-url").toString(),
                props.get("photo-url-prefix").toString(),
                out);

        new UpdateRelFinderConfig().updateExampleFile(props.get("bolt-url").toString(),
                props.get("bolt-user").toString(),
                props.get("bolt-passwd").toString(),
                new File(props.get("example-file-dir").toString()),
                props.get("example-person-name-list").toString().split(";"),
                out);

        new ExecuteProcedures().execute(props, out);
    }
}