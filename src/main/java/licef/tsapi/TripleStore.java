package licef.tsapi;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.update.*;
import licef.IOUtil;
import licef.LangUtil;
import licef.reflection.Invoker;
import licef.tsapi.model.NodeValue;
import licef.tsapi.model.Triple;
import licef.tsapi.model.Tuple;
import licef.tsapi.util.Translator;
import licef.tsapi.vocabulary.VocUtil;
import org.apache.jena.query.text.*;
import org.apache.lucene.store.FSDirectory;
import org.semarglproject.jena.rdf.rdfa.JenaRdfaReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-13
 */
public class TripleStore {

    public enum RdfaApi { JAVA_RDFA, SEMARGL };
    public enum RdfaFormat { RDFA_HTML, RDFA_XHTML };

    public static int READ_MODE = 0;
    public static int WRITE_MODE = 1;

    String namespace = "http://localhost/ns#";
    String databaseDir = "./data";
    String databaseName = "DB1";
    String databasePath;
    String serverDir = ".";
    Server server;
    Hashtable<String, Dataset> datasets;
    Hashtable<String, Dataset> datasetIndexes;
    Hashtable<String, HashSet<TextIndex>> textIndexes;

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

        datasets = new Hashtable<String, Dataset>();
        datasetIndexes = new Hashtable<String, Dataset>();
        textIndexes = new Hashtable<String, HashSet<TextIndex>>();
    }

    public void startServer(boolean readOnly) {
        if (server == null) {
            server = new Server(databasePath, serverDir, readOnly);
            server.start();
        }
    }

    public void stopServer() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }


    /****************************/
    /* Vocabularies, ontologies */
    /****************************/

    public void registerVocabulary(String vocUri, Class vocClass) {
        VocUtil.registerVocabulary(vocUri, vocClass);
    }

    /************/
    /* Datasets */
    /************/

    private Dataset getDataset() {
        String key = Thread.currentThread().toString();
        if (datasets.containsKey(key))
            return datasets.get(key);

        Dataset ds = TDBFactory.createDataset(databasePath);
        datasets.put(key, ds);
        return ds;
    }

    private Dataset getIndexDataset(Property[] predicatesToIndex, Object langInfo, String indexName) throws Exception {
        String thread = Thread.currentThread().toString();
        String key = "default";
        if (langInfo != null) {
            Object li = langInfo;
            if (langInfo instanceof String[]) //to ensure uniqueness
                li = new HashSet(Arrays.asList((String[])langInfo));
            key = li.toString();
        }
        if (indexName != null)
            key += indexName;
        key += thread;
        if (datasetIndexes.containsKey(key))
            return datasetIndexes.get(key);

        EntityDefinition entDef = new EntityDefinition("uri", "text");
        for (Property p : predicatesToIndex)
            entDef.set(VocUtil.getIndexFieldProperty(p), p.asNode());
        String extra = "";
        if (langInfo != null) {
            if (langInfo instanceof String)
                extra += "-" + langInfo;
            else
                extra += "-ml";
        }
        String indexPath = (indexName != null)?"/"+indexName:"/default";
        String path = this.databaseDir + "/lucene" + indexPath + extra;
        IOUtil.createDirectory(path);
        File dir = new File(path);
        TextIndex index;
        if (langInfo != null) {
            if (langInfo instanceof String)
                index = TextDatasetFactory.createLuceneIndexLocalized(FSDirectory.open(dir), entDef, langInfo.toString()) ;
            else
                index = TextDatasetFactory.createLuceneIndexMultiLingual(dir, entDef, (String[])langInfo) ;
        }
        else
            index = TextDatasetFactory.createLuceneIndex(FSDirectory.open(dir), entDef) ;


        Dataset res = TextDatasetFactory.create(getDataset(), index);

        //keep dataset reference
        datasetIndexes.put(key, res);

        //keep index(es) for end closing
        HashSet<TextIndex> set;
        if (textIndexes.containsKey(thread))
            set = textIndexes.get(thread);
        else {
            set = new HashSet<TextIndex>();
            textIndexes.put(thread, set);
        }

        if (index instanceof TextIndexLuceneMultiLingual) {
            for (TextIndex idx: ((TextIndexLuceneMultiLingual)index).getIndexes() )
                set.add(idx);
        }
        else
            set.add(index);

        return res;
    }

    private void manageIndexes(String action) {
        String thread = Thread.currentThread().toString();
        //lucene indexes
        if (textIndexes.containsKey(thread)) {
            for (Iterator it = textIndexes.get(thread).iterator(); it.hasNext(); ) {
                TextIndex index = (TextIndex) it.next();
                if ("commit".equals(action))
                    index.finishIndexing();
                else
                    index.abortIndexing();
            }
            //clear indexes
            textIndexes.remove(thread);
        }

        //clear indexDatasets
        for (Iterator it = datasetIndexes.keySet().iterator(); it.hasNext();) {
            String key = (String)it.next();
            if (key.endsWith(thread)) {
                Dataset ds = datasetIndexes.remove(key);
                ds.end();
            }
        }
    }

    /**************************/
    /* Transaction management */
    /**************************/

    /**
     * Execute a sequence of actions over dataset inside a transaction process
     * @param invoker start method for invocation
     * @throws Exception
     */
    public Object transactionalCall(Invoker invoker, int... mode) throws Exception {
        Object response;
        Dataset ds = getDataset();

        //call stack may have methods which recursively call this method
        if (ds.isInTransaction())
            return invoker.invoke();

        int _mode = (mode.length != 0)?mode[0]:0;
        if (_mode == WRITE_MODE)
            ds.begin(ReadWrite.WRITE);
        else
            ds.begin(ReadWrite.READ);
        try {
            response = invoker.invoke();
            if (_mode == WRITE_MODE)
                ds.commit();
            manageIndexes("commit");
        }
        finally {
            ds.end();
            manageIndexes("end");
        }
        return response;
    }

    /***********/
    /* General */
    /***********/

    public String[] getNamedGraphs() throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        ArrayList<String> names = new ArrayList<String>();
        for (Iterator it = ds.listNames(); it.hasNext();)
            names.add(it.next().toString());
        return names.toArray(new String[names.size()]);
    }


    /********************/
    /* Querying triples */
    /********************/

    public Triple[] getAllTriples(String... graphName) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        ArrayList<Triple> triples = new ArrayList<Triple>();
        Model model = (_graphName == null)?
                ds.getDefaultModel():
                ds.getNamedModel(getUri(_graphName));
        for (StmtIterator it = model.listStatements(); it.hasNext(); ) {
            Statement stmt = it.nextStatement();
            String subject = stmt.getSubject().getURI();
            String predicate = stmt.getPredicate().getURI();
            RDFNode _object = stmt.getObject();
            String object;
            String language = null;
            boolean isObjectLiteral = false;
            if (_object instanceof Literal) {
                language = ((Literal)_object).getLanguage();
                object = ((Literal)_object).getValue().toString();
                isObjectLiteral = true;
            }
            else
                object = _object.toString();
            triples.add(new Triple(subject, predicate, object, isObjectLiteral, language));
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public long getAllTriplesCount(String... graphName) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        Model model = (_graphName == null)?
                ds.getDefaultModel():
                ds.getNamedModel(getUri(_graphName));
        return model.size();
    }

    //s
    public Triple[] getTriplesWithSubject(String subject, String... graphName) throws Exception {
        return getTriplesWithSubjectPredicateObject(subject, null, null, false, null, graphName);
    }

    //p
    public Triple[] getTriplesWithPredicate(Property predicate, String... graphName) throws Exception {
        return getTriplesWithPredicate(predicate.getURI(), graphName);
    }

    public Triple[] getTriplesWithPredicate(String predicate, String... graphName) throws Exception {
        return getTriplesWithSubjectPredicateObject(null, predicate, null, false, null, graphName);
    }

    //sp
    public Triple[] getTriplesWithSubjectPredicate(String subject, Property predicate, String... graphName) throws Exception {
        return getTriplesWithSubjectPredicate(subject, predicate.getURI(), graphName);
    }

    public Triple[] getTriplesWithSubjectPredicate(String subject, String predicate, String... graphName) throws Exception {
        return getTriplesWithSubjectPredicateObject(subject, predicate, null, false, null, graphName);
    }

    //po
    public Triple[] getTriplesWithPredicateObject(Property predicate, String object, boolean isObjectLiteral, String language, String... graphName) throws Exception {
        return getTriplesWithPredicateObject(predicate.getURI(), object, isObjectLiteral, language, graphName);
    }

    public Triple[] getTriplesWithPredicateObject(String predicate, String object, boolean isObjectLiteral, String language, String... graphName) throws Exception {
        return getTriplesWithSubjectPredicateObject(null, predicate, object, isObjectLiteral, language, graphName);
    }

    //spo
    public Triple[] getTriplesWithSubjectPredicateObject(String subject, String predicate, String object, boolean isObjectLiteral, String language, String... graphName) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        ArrayList<Triple> triples = new ArrayList<Triple>();
        Node graph = (_graphName != null)?NodeFactory.createURI(getUri(_graphName)):NodeFactory.createURI("urn:x-arq:DefaultGraph");
        Node s = (subject != null)?NodeFactory.createURI(subject):Node.ANY;
        Node p = (predicate != null)?NodeFactory.createURI(predicate):Node.ANY;
        Node o = Node.ANY;

        if (object != null) {
            if (isObjectLiteral) {
                if (language != null && !"".equals(language))
                    o = NodeFactory.createLiteral(object, language, false);
                else
                    o = NodeFactory.createLiteral(object);
            }
            else
                o = NodeFactory.createURI(object);
        }

        Iterator<Quad> quadIter = ds.asDatasetGraph().find(graph, s, p, o) ;
        for (; quadIter.hasNext(); ) {
            Quad quad = quadIter.next();
            String resSubject = quad.getSubject().toString();
            String resPred = quad.getPredicate().toString();
            Node _object = quad.getObject();
            String resObj;
            String resLanguage = null;
            boolean resIsObjectLiteral = false;
            if (_object.isLiteral()) {
                resLanguage = _object.getLiteralLanguage();
                resObj = _object.getLiteralValue().toString();
                resIsObjectLiteral = true;
            }
            else
                resObj = _object.toString();
            triples.add(new Triple(resSubject, resPred, resObj, resIsObjectLiteral, resLanguage));
        }
        return triples.toArray(new Triple[triples.size()]);
    }



    /*******************/
    /* Content loading */
    /*******************/


    /**
     * Load content depending on format
     * @param is stream to be loaded
     * @param format RDFXML, TURTLE, N_TRIPLE or JSON constant integer
     * @param graphName target named graph. if null, default graph is used.
     * @throws Exception
     */

    public void loadContent(InputStream is, int format, String... graphName) throws Exception {
        loadContent(getDataset(), is, "", format, graphName);
    }

    /* with Lucene indexing */

    /**
     * Same args of precedent function + :
     * @param predicatesToIndex array of properties to index
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     * @param indexName if not null, separate lucene index is used with name indexName, "default" otherwise
     */

    public void loadContentWithTextIndexing(InputStream is, Property[] predicatesToIndex, Object langInfo, String indexName, int format, String... graphName) throws Exception {
        Dataset dataset = getIndexDataset(predicatesToIndex, langInfo, indexName);
        loadContent(dataset, is, "", format, graphName);
    }


    /* Load RDFa content */
    public void loadRDFa(RdfaApi api, RdfaFormat format, InputStream is, String baseUri, String... graphName) throws Exception {
        Dataset ds = getDataset();
        if( api == RdfaApi.JAVA_RDFA ) {
            Class.forName("net.rootdev.javardfa.jena.RDFaReader");
            loadContent(ds, is, baseUri, ( format == RdfaFormat.RDFA_XHTML ? Constants.XHTML : Constants.HTML ), graphName);
        }
        else if( api == RdfaApi.SEMARGL ) {
            JenaRdfaReader.inject();
            loadContent(ds, is, baseUri, Constants.RDFA, graphName);
        }
    }

    public void loadTurtle(InputStream is, String baseUri, String... graphName) throws Exception {
        loadContent(getDataset(), is, baseUri, Constants.TURTLE, graphName);
    }

    //Effective load
    private void loadContent(Dataset dataset, InputStream is, String baseUri, int format, String... graphName) throws Exception {
        if (!dataset.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        Model modelTmp = Translator.loadContent(is, baseUri, format);
        if (_graphName != null)
            dataset.addNamedModel(getUri(_graphName), modelTmp);
        else
            dataset.getDefaultModel().add(modelTmp);
    }

    /**
     * Load a TriG format file
     * @param path source file path
     * @param clearFirst if true, current TDB data are erased first,
     *                   else, new quad will be appended
     * @throws Exception
     */
    public void loadDataset(String path, int format, boolean clearFirst) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        Dataset externalDS = Translator.loadDataset(path, format);

        if (clearFirst) {
            //default graph
            clear();
            //named graphs
            for (Iterator it = externalDS.listNames(); it.hasNext(); ) {
                String name = (String) it.next();
                clear(name);
            }
        }

        //default graph
        ds.getDefaultModel().add(externalDS.getDefaultModel());
        //named graphs
        for(Iterator it = externalDS.listNames(); it.hasNext();) {
            String name = (String)it.next();
            ds.addNamedModel(name, externalDS.getNamedModel(name));
        }
    }

    /******************/
    /* Adding triples */
    /******************/

    public void insertTriple(Triple triple, String... graphName) throws Exception {
        ArrayList<Triple> triples = new ArrayList<Triple>();
        triples.add(triple);
        insertTriples(triples, graphName);
    }

    public void insertTriples(List<Triple> triples, String... graphName) throws Exception {
        insertTriples(getDataset(), triples, graphName);
    }

    /* with Lucene indexing */

    /**
     * @param predicatesToIndex array of properties to index
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     * @param indexName if not null, separate lucene index is used with name indexName, "default" otherwise
     */
    public void insertTripleWithTextIndexing(Triple triple, Property[] predicatesToIndex, Object langInfo, String indexName, String... graphName) throws Exception {
        ArrayList<Triple> triples = new ArrayList<Triple>();
        triples.add(triple);
        insertTriplesWithTextIndexing(triples, predicatesToIndex, langInfo, indexName, graphName);
    }

    public void insertTriplesWithTextIndexing(List<Triple> triples, Property[] predicatesToIndex, Object langInfo, String indexName, String... graphName) throws Exception {
        Dataset ids = getIndexDataset(predicatesToIndex, langInfo, indexName);
        insertTriples(ids, triples, graphName);
    }

    // Effective insert
    private void insertTriples(Dataset dataset, List<Triple> triples, String... graphName) throws Exception {
        if (!dataset.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        Model model = (_graphName == null)?
                dataset.getDefaultModel():
                ModelFactory.createDefaultModel();

        for (Triple triple : triples) {
            Property p = ResourceFactory.createProperty(triple.getPredicate());
            if (triple.isObjectLiteral()) {
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

        if (_graphName != null)
            dataset.addNamedModel(getUri(_graphName), model);
    }

    /********************/
    /* Removing triples */
    /********************/

    public void clear(String... graphName) throws Exception {
        clear(getDataset(), graphName);
    }

    /* with Lucene indexing */

    /**
     * @param indexedPredicates array of indexed properties
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     * @param indexName if not null, separate lucene index is used with name indexName, "default" otherwise
     */

    public void clearWithTextIndexing(Property[] indexedPredicates, Object langInfo, String indexName, String... graphName) throws Exception {
        Dataset dataset = getIndexDataset(indexedPredicates, langInfo, indexName);
        clear(dataset, graphName);
    }

    // Effective clear
    private void clear(Dataset dataset, String... graphName) throws Exception {
        if (!dataset.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        if (_graphName == null)
            dataset.getDefaultModel().removeAll();
        else
            dataset.removeNamedModel(getUri(_graphName));
    }

    public void removeTriple(Triple triple, String... graphName) throws Exception {
        ArrayList<Triple> triples = new ArrayList<Triple>();
        triples.add(triple);
        removeTriples(triples, graphName);
    }

    public void removeTriples(List<Triple> triples, String... graphName) throws Exception {
        removeTriples(getDataset(), triples, graphName);
    }

    /* with Lucene indexing */

    /**
     * @param indexedPredicates array of properties to index
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     * @param indexName if not null, separate lucene index is used with name indexName, "default" otherwise
     */

    public void removeTripleWithTextIndexing(Triple triple, Property[] indexedPredicates, Object langInfo, String indexName, String... graphName) throws Exception {
        ArrayList<Triple> triples = new ArrayList<Triple>();
        triples.add(triple);
        removeTriplesWithTextIndexing(triples, indexedPredicates, langInfo, indexName, graphName);
    }

    public void removeTriplesWithTextIndexing(List<Triple> triples, Property[] indexedPredicates, Object langInfo, String indexName, String... graphName) throws Exception {
        Dataset ids = getIndexDataset(indexedPredicates, langInfo, indexName);
        removeTriples(ids, triples, graphName);
    }

    // Effective remove
    private void removeTriples(Dataset dataset, List<Triple> triples, String... graphName) throws Exception {
        if (!dataset.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        Model model = (_graphName == null)?
                dataset.getDefaultModel():
                dataset.getNamedModel(getUri(_graphName));

        for (Triple triple : triples) {
            Resource subject = ResourceFactory.createResource(triple.getSubject());
            Property predicate = ResourceFactory.createProperty(triple.getPredicate());
            RDFNode object;
            if (triple.isObjectLiteral()) {
                if (triple.getLanguage() != null)
                    object = ResourceFactory.createLangLiteral(triple.getObject(), triple.getLanguage());
                else
                    object = ResourceFactory.createPlainLiteral(triple.getObject());
            }
            else
                object = ResourceFactory.createResource(triple.getObject());
            model.remove(subject, predicate, object);
        }
    }



    /**************************/
    /* SPARQL 1.1 Query forms */
    /**************************/

    public Tuple[] sparqlSelect(String queryString) throws Exception {
        return sparqlSelect(getDataset(), queryString);
    }

    /**
     * Sparql select with text index taken into account.
     * @param graphName if null, work on default graph
     * @param queryString
     * @param predicatesToIndex
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     * @param indexName if not null, separate lucene index is used with name indexName, "default" otherwise
     */
    public Tuple[] sparqlSelectWithTextIndexing(String queryString, Property[] predicatesToIndex, Object langInfo, String indexName, String... graphName) throws Exception {
        Dataset dataset = getIndexDataset(predicatesToIndex, langInfo, indexName);
        return sparqlSelect(dataset, queryString);
    }

    public Tuple[] sparqlSelect(Dataset dataset, String queryString) throws Exception {
        if (!dataset.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        //if indexed dataset, ctxt is set to null in Jena, so we must retrieve index in query execution
        if (dataset.asDatasetGraph() instanceof DatasetGraphText)
            TextDatasetFactory.setCtxtIndex(
                    (TextIndex) dataset.getContext().get(TextQuery.textIndex));

        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        try {
            ResultSet results = qexec.execSelect();
            List<String> varNames = results.getResultVars();
            for (; results.hasNext(); ) {
                QuerySolution res = results.nextSolution();
                Tuple tuple = new Tuple();
                for (Iterator it = varNames.iterator(); it.hasNext(); ) {
                    String varName = it.next().toString();
                    RDFNode n = res.get(varName);
                    NodeValue node = new NodeValue();
                    if (n == null)
                        node.setContent("");
                    else if (n.isLiteral()) {
                        node.setLiteral(true);
                        node.setContent((((Literal) n).getValue().toString()));
                        node.setLanguage(((Literal) n).getLanguage());
                    } else
                        node.setContent(n.toString());
                    tuple.setValue(varName, node);
                }
                tuple.setVarNames(varNames.toArray(new String[varNames.size()]));
                tuples.add(tuple);
            }
        } finally {
            qexec.close();
        }
        return tuples.toArray(new Tuple[tuples.size()]);
    }

    public boolean sparqlAsk(String queryString) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, ds);
        try {
            return qexec.execAsk();
        }
        finally {
            qexec.close() ;
        }
    }

    public Triple[] sparqlDescribe(String queryString) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        ArrayList<Triple> triples = new ArrayList<Triple>();
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, ds);
        for (Iterator it = qexec.execDescribeTriples(); it.hasNext(); ) {
            com.hp.hpl.jena.graph.Triple triple = (com.hp.hpl.jena.graph.Triple)it.next();
            String subject = triple.getSubject().getURI();
            String predicate = triple.getPredicate().getURI();
            com.hp.hpl.jena.graph.Node _object = triple.getObject();
            String object;
            String language = null;
            boolean isObjectLiteral = false;
            if (_object instanceof Node_Literal) {
                language = _object.getLiteralLanguage();
                object = _object.getLiteralValue().toString();
                isObjectLiteral = true;
            }
            else
                object = _object.getURI();
            triples.add(new Triple(subject, predicate, object, isObjectLiteral, language));
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public Triple[] sparqlConstruct(String queryString) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        ArrayList<Triple> triples = new ArrayList<Triple>();
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, ds);
        for (Iterator it = qexec.execConstructTriples(); it.hasNext(); ) {
            com.hp.hpl.jena.graph.Triple triple = (com.hp.hpl.jena.graph.Triple)it.next();
            String subject = triple.getSubject().getURI();
            String predicate = triple.getPredicate().getURI();
            com.hp.hpl.jena.graph.Node _object = triple.getObject();
            String object;
            String language = null;
            boolean isObjectLiteral = false;
            if (_object instanceof Node_Literal) {
                language = _object.getLiteralLanguage();
                object = _object.getLiteralValue().toString();
                isObjectLiteral = true;
            }
            else
                object = _object.getURI();
            triples.add(new Triple(subject, predicate, object, isObjectLiteral, language));
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public void sparqlUpdate(String updateString) throws Exception {
        sparqlUpdate(getDataset(), updateString);
    }

    /**
     * Sparql update with text index taken into account.
     * Limitation : Be careful to work only with one graph at a time on update query !!!
     * @param graphName if null, work on default graph
     * @param updateString
     * @param predicatesToIndex
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     * @param indexName if not null, separate lucene index is used with name indexName, "default" otherwise
     */
    public void sparqlUpdateWithTextIndexing(String updateString, Property[] predicatesToIndex, Object langInfo, String indexName, String... graphName) throws Exception {
        Dataset dataset = getIndexDataset(predicatesToIndex, langInfo, indexName);
        sparqlUpdate(dataset, updateString);
    }

    //Effective sparqlUpdate
    private void sparqlUpdate(Dataset dataset, String updateString) throws Exception {
        if (!dataset.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        GraphStore graphStore = GraphStoreFactory.create(dataset);
        UpdateRequest request = UpdateFactory.create(updateString);
        UpdateProcessor proc = UpdateExecutionFactory.create(request, graphStore);
        proc.execute();
    }


    /*************/
    /* Inference */
    /*************/

    public void doInference(String graphName, String graphSchema) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        Model modelSchema = ds.getNamedModel(getUri(graphSchema));
        Model modelData = ds.getNamedModel(getUri(graphName));
        Reasoner reasoner = ReasonerRegistry.getOWLMicroReasoner();
        reasoner = reasoner.bindSchema(modelSchema);
        InfModel infModel = ModelFactory.createInfModel(reasoner, modelData);
        modelData.add(infModel.listStatements());
    }



    /********************/
    /* Misc. operations */
    /********************/

    public String getUri(String element) {
        if (element != null && !"".equals(element) && !element.startsWith("http://"))
            element = namespace + element;
        return element;
    }

    public boolean isResourceExists(String uri) throws Exception {
        return isResourceExists(uri, null);
    }

    public boolean isResourceExists(String uri, String graphName) throws Exception {
        return getTriplesWithSubject(uri, graphName).length > 0;
    }

    /**
     * Retrieve the best literal with following ranking :
     * 1 - With exactly same language
     * 2 - With first similar language (ex: fr-CA for a fr request)
     * 3 - unlocalized literal when no matching language
     * 4 - default case : first found result
     * @return Array of 2 Strings: the first is the best literal, the second is its language.
     */

    public String[] getBestLocalizedLiteralObject(String uri, Property predicate, String lang, String... graphName) throws Exception {
        return getBestLocalizedLiteralObject(uri, predicate.getURI(), lang, graphName);
    }

    public String[] getBestLocalizedLiteralObject(String uri, String predicate, String lang, String... graphName) throws Exception {
        lang = LangUtil.convertLangToISO2(lang);
        Triple[] triples = getTriplesWithSubjectPredicate(uri, predicate, graphName);
        if (lang == null)
            lang = "###"; //to force unlocalized choice
        String[] res = null;
        boolean foundWithSimilarLanguage = false;
        boolean foundWithoutLanguage = false;
        for (Triple t: triples) {
            if (t.getLanguage() != null && t.getLanguage().toLowerCase().equals(lang.toLowerCase())) {
                res = new String[] { t.getObject(), t.getLanguage() };
                break;
            }
            if (t.getLanguage() != null &&
                    ( (t.getLanguage().toLowerCase().startsWith(lang.toLowerCase())) ||
                            (lang.startsWith(t.getLanguage().toLowerCase())) ) &&
                    !foundWithSimilarLanguage) {
                res = new String[] { t.getObject(), t.getLanguage() };
                foundWithSimilarLanguage = true;
            }
            if (t.getLanguage() == null && !foundWithSimilarLanguage && !foundWithoutLanguage) {
                res = new String[] { t.getObject(), t.getLanguage() };
                foundWithoutLanguage = true;
            }
            if (res == null)
                res = new String[] { t.getObject(), t.getLanguage() };
        }
        return res;
    }

    /**
     * Dump a graph on disk
     * @param outputFile
     * @param format integer constant
     */
    public void dump(String outputFile, int format, String... graphName) throws Exception {
        Dataset ds = getDataset();
        if (!ds.isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        String _graphName = (graphName.length != 0)?graphName[0]:null;
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(outputFile);
            Model model = (_graphName == null)?
                    ds.getDefaultModel():
                    ds.getNamedModel(getUri(_graphName));
            Translator.translate(model, format, os);
        } finally {
            if( os != null )
                os.close();
        }
    }

    /**
     * Dump whole dataset on disk in TriG format
     * @param outputFile
     */
    public void dumpDataset(String outputFile, int format) throws Exception {
        if (!getDataset().isInTransaction())
            throw new Exception("Cannot perform action on triple store without transaction.");

        FileOutputStream os = null;
        try {
            os = new FileOutputStream(outputFile);
            Translator.translate(getDataset(), format, os);
        } finally {
            if( os != null )
                os.close();
        }
    }


}
