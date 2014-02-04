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
import licef.tsapi.model.NodeValue;
import licef.tsapi.model.Triple;
import licef.tsapi.model.Tuple;
import licef.tsapi.util.Translator;
import licef.tsapi.util.Util;
import licef.tsapi.vocabulary.VocUtil;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextDatasetFactory;
import org.apache.jena.query.text.TextIndex;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.semarglproject.jena.rdf.rdfa.JenaRdfaReader;


/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-13
 */
public class TripleStore {

    public enum RdfaApi { JAVA_RDFA, SEMARGL };
    public enum RdfaFormat { RDFA_HTML, RDFA_XHTML };

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

    public void startServer(boolean readOnly) {
        if (server == null) {
            server = new Server(databasePath, serverDir, readOnly);
            server.start();
        }
    }


    /****************************/
    /* Vocabularies, ontologies */
    /****************************/

    public void registerVocabulary(String vocUri, Class vocClass) {
        VocUtil.registerVocabulary(vocUri, vocClass);
    }


    /**********************/
    /* Dataset operations */
    /**********************/

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


    /* Association with Lucene indexing */

    private Dataset createIndexDataset(Property[] predicatesToIndex, Object langInfo, String... graphName) throws Exception {
        String _graphName = (graphName.length != 0)?graphName[0]:null;
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
        String graph = (_graphName != null)?"/graphs/"+_graphName:"/default";
        String path = this.databaseDir + "/lucene" + graph + extra;
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
        Dataset dataset = TDBFactory.createDataset(databasePath);
        return TextDatasetFactory.create(dataset, index);
    }

    /********************/
    /* Querying triples */
    /********************/

    public Triple[] getAllTriples(String... graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.READ) ;
        ArrayList<Triple> triples = new ArrayList<Triple>();
        try {
            Model model = (_graphName == null)?
                dataset.getDefaultModel():
                dataset.getNamedModel(getUri(_graphName));
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
        }
        finally {
            dataset.end() ;
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public long getAllTriplesCount(String... graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.READ);
        try {
            Model model = (_graphName == null)?
                    dataset.getDefaultModel():
                    dataset.getNamedModel(getUri(_graphName));
            return model.size();
        }
        finally {
            dataset.end() ;
        }
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
        Dataset dataset = TDBFactory.createDataset(databasePath);
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.READ);
        ArrayList<Triple> triples = new ArrayList<Triple>();
        try {
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

            Iterator<Quad> quadIter = dataset.asDatasetGraph().find(graph, s, p, o) ;
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
        }
        finally {
            dataset.end();
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
        Dataset dataset = TDBFactory.createDataset(databasePath);
        loadContent(dataset, is, "", format, graphName);
    }

    /* with Lucene indexing */

    /**
     * Same args of precedent function + :
     * @param predicatesToIndex array of properties to index
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     */

    public void loadContentWithTextIndexing(InputStream is, Property[] predicatesToIndex, Object langInfo, int format, String... graphName) throws Exception {
        Dataset dataset = createIndexDataset(predicatesToIndex, langInfo, graphName);
        loadContent(dataset, is, "", format, graphName);
    }


    /* Load RDFa content */
    public void loadRDFa(RdfaApi api, RdfaFormat format, InputStream is, String baseUri, String... graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        
        if( api == RdfaApi.JAVA_RDFA ) {
            Class.forName("net.rootdev.javardfa.jena.RDFaReader");
            loadContent(dataset, is, baseUri, ( format == RdfaFormat.RDFA_XHTML ? Constants.XHTML : Constants.HTML ), graphName);
        }
        else if( api == RdfaApi.SEMARGL ) {
            JenaRdfaReader.inject();
            loadContent(dataset, is, baseUri, Constants.RDFA, graphName);
        }
    }
    
    public void loadTurtle(InputStream is, String baseUri, String... graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        loadContent(dataset, is, baseUri, Constants.TURTLE, graphName);
    }

    //Effective load
    private void loadContent(Dataset dataset, InputStream is, String baseUri, int format, String... graphName) throws Exception {
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.WRITE) ;
        try {
            Model modelTmp = Translator.loadContent(is, baseUri, format);
            if (_graphName != null)
                dataset.addNamedModel(getUri(_graphName), modelTmp);
            else
                dataset.getDefaultModel().add(modelTmp);
            dataset.commit();
        }
        finally {
            dataset.end();
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
        Dataset dataset = TDBFactory.createDataset(databasePath);
        insertTriples(dataset, triples, graphName);
    }

    /* with Lucene indexing */

    /**
     * @param predicatesToIndex array of properties to index
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     */
    public void insertTripleWithTextIndexing(Triple triple, Property[] predicatesToIndex, Object langInfo, String... graphName) throws Exception {
        ArrayList<Triple> triples = new ArrayList<Triple>();
        triples.add(triple);
        insertTriplesWithTextIndexing(triples, predicatesToIndex, langInfo, graphName);
    }

    public void insertTriplesWithTextIndexing(List<Triple> triples, Property[] predicatesToIndex, Object langInfo, String... graphName) throws Exception {
        Dataset dataset = createIndexDataset(predicatesToIndex, langInfo, graphName);
        insertTriples(dataset, triples, graphName);
    }

    // Effective insert
    private void insertTriples(Dataset dataset, List<Triple> triples, String... graphName) throws Exception {
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.WRITE) ;
        try {
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

            dataset.commit();
        }
        finally {
            dataset.end();
        }
    }

    /********************/
    /* Removing triples */
    /********************/

    public void clear(String... graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        clear(dataset, graphName);
    }

    /* with Lucene indexing */

    /**
     * @param indexedPredicates array of indexed properties
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     */

    public void clearWithTextIndexing(Property[] indexedPredicates, Object langInfo, String... graphName) throws Exception {
        Dataset dataset = createIndexDataset(indexedPredicates, langInfo, graphName);
        clear(dataset, graphName);
    }

    // Effective clear
    private void clear(Dataset dataset, String... graphName) throws Exception {
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.WRITE);
        try {
            if (_graphName == null)
                dataset.getDefaultModel().removeAll();
            else
                dataset.removeNamedModel(getUri(_graphName));
            dataset.commit() ;
        }
        finally {
            dataset.end();
        }
    }

    public void removeTriple(Triple triple, String... graphName) throws Exception {
        ArrayList<Triple> triples = new ArrayList<Triple>();
        triples.add(triple);
        removeTriples(triples, graphName);
    }

    public void removeTriples(List<Triple> triples, String... graphName) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        removeTriples(dataset, triples, graphName);
    }

    /* with Lucene indexing */

    /**
     * @param indexedPredicates array of properties to index
     * @param langInfo formats are :
     *                 - null for standard indexing
     *                 - langstring for localized indexing (ex: "fr")
     *                 - array of langstrings for multi-lingual indexing (ex: ["fr", "en"])
     */

    public void removeTripleWithTextIndexing(Triple triple, Property[] indexedPredicates, Object langInfo, String... graphName) throws Exception {
        ArrayList<Triple> triples = new ArrayList<Triple>();
        triples.add(triple);
        removeTriplesWithTextIndexing(triples, indexedPredicates, langInfo, graphName);
    }

    public void removeTriplesWithTextIndexing(List<Triple> triples, Property[] indexedPredicates, Object langInfo, String... graphName) throws Exception {
        Dataset dataset = createIndexDataset(indexedPredicates, langInfo, graphName);
        removeTriples(dataset, triples, graphName);
    }

    // Effective remove
    private void removeTriples(Dataset dataset, List<Triple> triples, String... graphName) throws Exception {
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.WRITE) ;
        try {
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

            dataset.commit();
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
                    NodeValue node = new NodeValue();
                    if (n instanceof Literal) {
                        node.setLiteral(true);
                        node.setContent((((Literal)n).getValue().toString()));
                        node.setLanguage(((Literal)n).getLanguage());
                    }
                    else
                        node.setContent(n.toString());
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
        }
        finally {
            dataset.end() ;
        }
        return triples.toArray(new Triple[triples.size()]);
    }

    public void sparqlUpdate(String updateString) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        sparqlUpdate(dataset, updateString);
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
     */
    public void sparqlUpdateWithTextIndexing(String updateString, Property[] predicatesToIndex, Object langInfo, String... graphName) throws Exception {
        Dataset dataset = createIndexDataset(predicatesToIndex, langInfo, graphName);
        sparqlUpdate(dataset, updateString);
    }

    //Effective sparqlUpdate
    private void sparqlUpdate(Dataset dataset, String updateString) throws Exception {
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




    /*************/
    /* Inference */
    /*************/

    public void doInference(String graphName, String graphSchema) throws Exception {
        Dataset dataset = TDBFactory.createDataset(databasePath);
        dataset.begin(ReadWrite.WRITE) ;

        Model modelSchema = dataset.getNamedModel(getUri(graphSchema));
        Model modelData = dataset.getNamedModel(getUri(graphName));
        try {
            Reasoner reasoner = ReasonerRegistry.getOWLMicroReasoner();
            reasoner = reasoner.bindSchema(modelSchema);
            InfModel infModel = ModelFactory.createInfModel(reasoner, modelData);
            modelData.add(infModel.listStatements());
            dataset.commit();
        } finally {
            dataset.end();
        }
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
     * @returns Array of 2 Strings: the first is the best literal, the second is its language.
     */

    public String[] getBetterLocalizedLiteralObject(String uri, Property predicate, String lang, String... graphName) throws Exception {
        return getBetterLocalizedLiteralObject(uri, predicate.getURI(), lang, graphName);
    }

    public String[] getBetterLocalizedLiteralObject(String uri, String predicate, String lang, String... graphName) throws Exception {
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
        Dataset dataset = TDBFactory.createDataset(databasePath);
        String _graphName = (graphName.length != 0)?graphName[0]:null;
        dataset.begin(ReadWrite.WRITE);
        try {
            Model model = (_graphName == null)?
                    dataset.getDefaultModel():
                    dataset.getNamedModel(getUri(_graphName));
            Translator.translate(model, format, new FileOutputStream(outputFile));
        } finally {
            dataset.end();
        }
    }

}
