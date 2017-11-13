import honglou.SyncTool;

/**
 * Created by bluejoe on 2017/11/12.
 */
public class Test1 {
    public void test1(){
        int[] arr = SyncTool.sync("bolt://222.223.208.25:7687", "neo4j", "neoneo", "http://222.223.208.25:8088/fuseki/honglou");
    }
}
