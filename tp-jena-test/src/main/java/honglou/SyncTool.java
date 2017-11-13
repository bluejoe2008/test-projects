package honglou;

/**
 * Created by bluejoe on 2017/11/12.
 */
public abstract class SyncTool {
    public static int[] sync(String neo4jBoltUrl, String user, String password,
                      String sparqlUpdateUrl) {
        return neo4j2rdf.sync(neo4jBoltUrl, user, password, sparqlUpdateUrl);
    }
}