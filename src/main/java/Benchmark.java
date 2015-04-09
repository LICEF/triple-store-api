import licef.reflection.Invoker;
import licef.tsapi.Constants;
import licef.tsapi.TripleStore;
import licef.tsapi.model.Triple;
import licef.tsapi.vocabulary.SKOS;


/**
 * Created by amiara on 2015-04-07.
 */
public class Benchmark {
    static TripleStore ts;
    static String ns = "http://localhost/";
    public static void main(String[] args) {
        try {
            String dbPath = "e:/zzz/tsapiTest2/data";
//            String dbPath = "e:/proeaf/database";
            String serverPath = "e:/zzz/tsapiTest2";
            ts = new TripleStore(dbPath, serverPath, null);
            ts.startServer(false);


//            Invoker inv = new Invoker(null, "Benchmark", "transactionCall", new Object[]{});
//            ts.transactionalCall(inv, TripleStore.WRITE_MODE);
//
//            Invoker inv = new Invoker(null, "Benchmark", "transactionCall2", new Object[]{});
//            ts.transactionalCall(inv, TripleStore.WRITE_MODE);


            Invoker inv = new Invoker(ts, "licef.tsapi.TripleStore", "dumpDataset",
                    new Object[]{"e:/zzz/tsapiTest2/output.trig", Constants.TRIG});
            ts.transactionalCall(inv);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static void transactionCall() throws Exception {
        System.out.println("\n\nTRANSACTION");
        generateTriples("root", 0, 6);
    }

    public static void transactionCall2() throws Exception {
        //inference
        String query = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n " +
                "INSERT { ?s skos:narrowerTransitive ?c }\n " +
                "WHERE { ?s skos:narrower+ ?c }";

        ts.sparqlUpdate(query);
    }

    private static void generateTriples(String parent, int level, int depth) throws Exception{
        if (level < depth) {
            for (int i = 0; i < 10; i++) {
                String child = level + "-" + (("root".equals(parent))?"":parent+"-")  + i;
//                Triple t = new Triple(ns + parent, SKOS.narrower, ns + child);
//                ts.insertTriple(t);
                Triple t = new Triple(ns + "other5", SKOS.narrowerTransitive, ns + child);
                ts.insertTriple(t);
                generateTriples(child, level + 1, depth);
            }
        }
    }
}
