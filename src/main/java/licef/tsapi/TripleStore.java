package licef.tsapi;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.update.*;
import licef.IOUtil;
import licef.reflection.Invoker;
import licef.reflection.ThreadInvoker;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.FusekiCmd;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-13
 */
public class TripleStore {

    String databasePath = "DB1";

    public TripleStore() {
    }

    public TripleStore(String databasePath) {
        this.databasePath = databasePath;
    }

    public String getDatabasePath() {
        return databasePath;
    }

    public void setDatabasePath(String databasePath) {
        this.databasePath = databasePath;
    }

    public void start() {
        IOUtil.createDirectory(databasePath);
        TDBFactory.createDataset(databasePath);
        (new ThreadInvoker(new Invoker(this, "licef.tsapi.TripleStore",
                "startFuseki", new Object[]{}))).start();
    }

    public void startFuseki() {
        System.out.println("Start Fuseki server...");
        Fuseki.init() ;
        String fusekiServerPages = null;
        try {
            fusekiServerPages = this.getClass().getResource("/pages").getFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        new FusekiCmd("--update", "--loc=" + databasePath, "--pages=" + fusekiServerPages, "/ds").mainRun() ;
    }

    public void clear() {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE) ;
        Model model = dataset.getDefaultModel();
        try {
            model.removeAll();
            dataset.commit() ;
            System.out.println("Default model cleared.");
        } finally {
            dataset.end();
        }
    }

    public void listTriples() {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.READ) ;
        try {
            String queryString =
                    "SELECT ?s ?p ?o " +
                    "WHERE { " +
                    "    ?s ?p ?o . " +
                    "} " ;
            executeQuery(queryString, dataset);
        } finally {
            dataset.end() ;
        }
    }

    void executeQuery(String queryString, Dataset dataset) {
        Query query = QueryFactory.create(queryString) ;
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
        try {
            ResultSet results = qexec.execSelect() ;
            int i = 0;
            for ( ; results.hasNext() ; i++)
            {
                QuerySolution soln = results.nextSolution() ;
                System.out.println("soln = " + soln);
            }
            System.out.println(i + " triples");
        } finally { qexec.close() ; }
    }


    public void insertTriples(List<Triple> triples) {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE) ;
        try {
            StringBuilder queryString = new StringBuilder();
            queryString.append("INSERT DATA {");
            for (Triple triple : triples) {
                queryString.append(triple + " . ");
            }
            queryString.append("}");
            System.out.println("queryString = " + queryString);
            GraphStore graphStore = GraphStoreFactory.create(dataset);
            UpdateRequest request = UpdateFactory.create(queryString.toString());
            UpdateProcessor proc = UpdateExecutionFactory.create(request, graphStore);
            proc.execute();
            dataset.commit();
        } catch (Exception e) {
            dataset.end();
        }
    }
}
