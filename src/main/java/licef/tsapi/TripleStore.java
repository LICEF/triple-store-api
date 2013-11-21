package licef.tsapi;

import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.update.*;
import licef.IOUtil;
import licef.tsapi.model.Literal;
import licef.tsapi.model.*;
import licef.tsapi.model.Resource;
import licef.tsapi.util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-13
 */
public class TripleStore {

    String namespace = "http://localhost/ns#";
    String databaseDir = "./data";
    String databaseName = "DB1";
    String databasePath;
    String serverDir = ".";
    Server server;

    public TripleStore() {
        databasePath = databaseDir + "/" + databaseName;
        IOUtil.createDirectory(this.databasePath);
    }

    public TripleStore(String databaseDir, String serverDir, String namespace) {
        if (databaseDir != null)
            this.databaseDir = databaseDir;
        if (serverDir != null)
            this.serverDir = serverDir;
        if (namespace != null)
            this.namespace = namespace;
        databasePath = databaseDir + "/" + databaseName;
        IOUtil.createDirectory(this.databasePath);
    }

    public void startServer() {
        if (server == null) {
            server = new Server(databasePath, serverDir);
            server.start();
        }
    }

    String getUri(String element) {
        if (element != null && !"".equals(element) &&
                !element.startsWith("http://") && !element.startsWith(namespace))
            element = namespace + element;
        return element;
    }

    public String[] getNamedGraphs() throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.READ) ;
        ArrayList<String> names = new ArrayList<String>();
        try {
            for (Iterator it = dataset.listNames(); it.hasNext();)
                names.add(it.next().toString());
        }
        finally {
            dataset.end() ;
        }
        return names.toArray(new String[names.size()]);
    }


    /********************/
    /* Querying triples */
    /********************/


    public Triple[] getAllTriples() throws Exception {
        return getAllTriples(null);
    }

    public Triple[] getAllTriples(String graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.READ) ;
        ArrayList<Triple> triples = new ArrayList<Triple>();
        try {
            Model model = (graphName == null)?
                dataset.getDefaultModel():
                dataset.getNamedModel(getUri(graphName));
            for (StmtIterator it = model.listStatements(); it.hasNext(); ) {
                Statement stmt = it.nextStatement();
                String subject = stmt.getSubject().getURI();
                Property predicate = stmt.getPredicate();
                RDFNode _object = stmt.getObject();
                String object;
                String language = null;
                if (_object instanceof com.hp.hpl.jena.rdf.model.Literal) {
                    language = ((com.hp.hpl.jena.rdf.model.Literal)_object).getLanguage();
                    object = ((com.hp.hpl.jena.rdf.model.Literal)_object).getValue().toString();
                }
                else
                    object = _object.toString();
                triples.add(new Triple(subject, predicate.getURI(), object, language));
            }
        }
        finally {
            dataset.end() ;
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public long getAllTriplesCount() throws Exception {
        return getAllTriplesCount(null);
    }

    public long getAllTriplesCount(String graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.READ);
        try {
            Model model = (graphName == null)?
                    dataset.getDefaultModel():
                    dataset.getNamedModel(getUri(graphName));
            return model.size();
        }
        finally {
            dataset.end() ;
        }
    }


    /******************/
    /* Adding triples */
    /******************/

    public void loadRdf(String file) throws Exception {
        loadRdf(file, null);
    }

    public void loadRdf(String file, String graphName) throws Exception {
        loadRdf(new FileInputStream(file), graphName);
    }

    public void loadRdf(InputStream is) throws Exception {
        loadRdf(is, null);
    }

    public void loadRdf(InputStream is, String graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE) ;
        try {
            Model modelTmp = ModelFactory.createDefaultModel();
            modelTmp.read(is, null);
            if (graphName != null)
                dataset.addNamedModel(getUri(graphName), modelTmp);
            else
                dataset.getDefaultModel().add(modelTmp);
            dataset.commit();
        }
        finally {
            dataset.end();
        }

    }

    public void insertTriples(List<Triple> triples) throws Exception {
        insertTriples(triples, null);
    }

    public void insertTriples(List<Triple> triples, String graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE) ;
        try {
            Model model = (graphName == null)?
                    dataset.getDefaultModel():
                    ModelFactory.createDefaultModel();

            for (Triple triple : triples) {
                Property p = Util.getProperty(triple.getPredicate());
                if (triple.isLiteral()) {
                    if (triple.getLanguage() != null)
                        model.createResource(triple.getSubject()).addProperty(p,
                                model.createLiteral(triple.getObject(), triple.getLanguage()));
                    else
                        model.createResource(triple.getSubject()).addProperty(p,
                                model.createLiteral(triple.getObject()));
                }
                else
                    model.createResource(triple.getSubject()).addProperty(p,
                            model.createResource(triple.getObject()));
            }

            if (graphName != null)
                dataset.addNamedModel(getUri(graphName), model);

            dataset.commit();
        }
        finally {
            dataset.end();
        }
    }

    /********************/
    /* Removing triples */
    /********************/

    public void clear() throws Exception {
        clear(null);
    }

    public void clear(String graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE);
        try {
            if (graphName == null)
                dataset.getDefaultModel().removeAll();
            else
                dataset.removeNamedModel(getUri(graphName));
            dataset.commit() ;
        }
        finally {
            dataset.end();
        }
    }


    /**************************/
    /* SPARQL 1.1 Query forms */
    /**************************/

    public Tuple[] sparqlSelect(String queryString) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.READ);
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        try {
            String[] varNames = null;
            ArrayList<String> vars = new ArrayList<String>();
            for (ResultSet results = qexec.execSelect(); results.hasNext();) {
                QuerySolution res = results.nextSolution();
                Tuple tuple = new Tuple();
                for (Iterator it = res.varNames(); it.hasNext();) {
                    String varName = it.next().toString();
                    if (varNames == null)
                        vars.add(varName);
                    RDFNode n = res.get(varName);
                    Node node;
                    if (n instanceof com.hp.hpl.jena.rdf.model.Literal)
                        node = new Literal(((com.hp.hpl.jena.rdf.model.Literal)n).getValue().toString(),
                                ((com.hp.hpl.jena.rdf.model.Literal)n).getLanguage());
                    else
                        node = new Resource(n.toString());
                    tuple.setValue(varName, node);
                }
                if (varNames == null)
                    varNames = vars.toArray(new String[vars.size()]);
                tuple.setVarNames(varNames);
                tuples.add(tuple);
            }
        }
        finally {
            qexec.close() ;
            dataset.end();
        }
        return tuples.toArray(new Tuple[tuples.size()]);
    }

    public boolean sparqlAsk(String queryString) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.READ);
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        try {
            return qexec.execAsk();
        }
        finally {
            qexec.close() ;
            dataset.end();
        }
    }

    public Triple[] sparqlDescribe(String queryString) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE);
        ArrayList<Triple> triples = new ArrayList<Triple>();
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        try {

            for (Iterator it = qexec.execDescribeTriples(); it.hasNext(); ) {
                com.hp.hpl.jena.graph.Triple triple = (com.hp.hpl.jena.graph.Triple)it.next();
                String subject = triple.getSubject().getURI();
                String predicate = triple.getPredicate().getURI();
                com.hp.hpl.jena.graph.Node _object = triple.getObject();
                String object;
                String language = null;
                if (_object instanceof Node_Literal) {
                    language = _object.getLiteralLanguage();
                    object = _object.getLiteralValue().toString();
                }
                else
                    object = _object.getURI();
                triples.add(new Triple(subject, predicate, object, language));
            }
        }
        finally {
            dataset.end() ;
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public Triple[] sparqlConstruct(String queryString) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE);
        ArrayList<Triple> triples = new ArrayList<Triple>();
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        try {

            for (Iterator it = qexec.execConstructTriples(); it.hasNext(); ) {
                com.hp.hpl.jena.graph.Triple triple = (com.hp.hpl.jena.graph.Triple)it.next();
                String subject = triple.getSubject().getURI();
                String predicate = triple.getPredicate().getURI();
                com.hp.hpl.jena.graph.Node _object = triple.getObject();
                String object;
                String language = null;
                if (_object instanceof Node_Literal) {
                    language = _object.getLiteralLanguage();
                    object = _object.getLiteralValue().toString();
                }
                else
                    object = _object.getURI();
                triples.add(new Triple(subject, predicate, object, language));
            }
        }
        finally {
            dataset.end() ;
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public void sparqlUpdate(String updateString) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE) ;
        try {
            GraphStore graphStore = GraphStoreFactory.create(dataset);
            UpdateRequest request = UpdateFactory.create(updateString);
            UpdateProcessor proc = UpdateExecutionFactory.create(request, graphStore);
            proc.execute();

            dataset.commit();
        }
        finally {
            dataset.end();
        }
    }


    //to de removed
    public Hashtable<String, String>[] getResults( String queryName, Object... params ) throws Exception {
        return null;
    }
    public int getResultsCount(String queryName, Object... params) throws Exception {
        return -1;
    }


}
