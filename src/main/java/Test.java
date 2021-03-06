import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.tdb.TDBFactory;
import licef.reflection.Invoker;
import licef.tsapi.Constants;
import licef.tsapi.model.NodeValue;
import licef.tsapi.model.Triple;
import licef.tsapi.TripleStore;
import licef.tsapi.model.Tuple;
import licef.tsapi.textIndex.IndexConfig;
import licef.tsapi.util.Util;
import licef.tsapi.vocabulary.DCTERMS;
import licef.tsapi.vocabulary.FOAF;
import licef.tsapi.vocabulary.SKOS;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-15
 */
public class Test {
    static TripleStore ts;
    public static void main(String[] args) {
        try {
        String dbPath = "e:/zzz/tsapiTest2/data";
//            String dbPath = "e:/proeaf/database";
            String serverPath = "e:/zzz/tsapiTest2";
            ts = new TripleStore(dbPath, serverPath, null);
//        ts.startServer(true);


//            ts.loadContent(new FileInputStream("e:/zzz/tsapiTest/output.ttl"), Constants.TURTLE, "vv");
//            ts.loadContent(new FileInputStream("e:/zzz/tsapiTest/output.n3"), Constants.N_TRIPLE);
//            ts.loadRDFa(TripleStore.RdfaApi.JAVA_RDFA, TripleStore.RdfaFormat.RDFA_HTML, new URL("http://www.3kbo.com/examples/rdfa/simple.html").openStream(), "http://local1");
//            ts.loadRDFa(TripleStore.RdfaApi.JAVA_RDFA, TripleStore.RdfaFormat.RDFA_HTML, new URL("http://examples.tobyinkster.co.uk/hcard").openStream(), "http://local2");
//            ts.loadRDFa(TripleStore.RdfaApi.JAVA_RDFA, TripleStore.RdfaFormat.RDFA_HTML, new FileInputStream("e:/zzz/tsapiTest/big.htm"), "http://local3");


//            ts.loadContentWithTextIndexing(new FileInputStream("e:/zzz/tsapiTest/lom1.7.rdf"),
//                    new Property[]{SKOS.prefLabel}, new String[]{"fr", "en"}, null, Constants.RDFXML);

        launchControlPanel(ts);

            ArrayList<Triple> l = new ArrayList<Triple>();
//            Triple t1 = new Triple("http://uri/res 10", SKOS.broadMatch, "http://uri/res2");
            Triple t0 = new Triple("http://uri/res122", SKOS.prefLabel, "Hello", "en");
            Triple t2 = new Triple("http://uri/res122", SKOS.prefLabel, "bye");
            Triple t3 = new Triple("http://uri/res122", SKOS.prefLabel, "bonjour les amis", "fr");
            Triple t4 = new Triple("http://uri/res222", SKOS.prefLabel, "comment allez vous les amis", "fr");
//            Triple t5 = new Triple("http://uri/res2", SKOS.broadMatch, "http://uri/res3");
            l.add(t0);
//            l.add(t1);
            l.add(t2);
            l.add(t3);
            l.add(t4);
//            l.add(t5);


//            ts.insertTriples(l);
//            ts.insertTriplesWithTextIndexing(l, new Property[]{SKOS.prefLabel}, "fr");
//            ts.insertTriplesWithTextIndexing(l, new Property[]{SKOS.prefLabel}, new String[]{"fr", "en"}, null);


//            ts.removeTriplesWithTextIndexing(l, new Property[]{SKOS.prefLabel}, new String[]{"fr", "en"});
//            ts.insertTriplesWithTextIndexing(l, new Property[]{SKOS.prefLabel}, null);
//            ts.insertTriplesWithTextIndexing(l, new Property[]{}, new String[]{"fr", "en"}, null);
//            Triple[] toDelete = ts.getTriplesWithSubjectPredicate("http://uri/res1", SKOS.broadMatch, "testNamed");
//            ts.removeTriples(Arrays.asList(toDelete), "testNamed");

//            boolean b = ts.isResourceExists("http://uri/res1", "named");
//            System.out.println("b = " + b);

//            Triple[] tt = ts.getTriplesWithPredicate(SKOS.broadMatch);
            /*Triple[] tt = ts.getTriplesWithSubject("http://uri/res1", "testNamed");
            for (Triple t : tt) {
                System.out.println("t = " + t);
            }*/


            /*ArrayList<Triple> l = new ArrayList<Triple>();
            Triple t1 = new Triple("http://uri/res1", SKOS.broadMatch, "http://uri/res2");
            Triple t2 = new Triple("http://uri/res1", SKOS.prefLabel, "Hello");
            Triple t3 = new Triple("http://uri/res1", SKOS.altLabel, "bonjour les amis", "fr");
            l.add(t1);
            l.add(t2);
            l.add(t3);
            ts.insertTriples(l);*/




            //SELECT
            /*String q = "SELECT * WHERE { ?s ?p ?o . } ";
            Tuple[] ttu = ts.sparqlSelect(q);
            for (Tuple ssu : ttu) {
                System.out.println("n = " + ssu);
            }*/

            /*String q = "PREFIX text: <http://jena.apache.org/text#>\n" +
                    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                    "SELECT * " +
                    "WHERE { " +
                    "(?uri ?score) text:query (skos:prefLabel 'comment' 'lang:fr' ) .\n" +
                    "?uri skos:prefLabel ?label .\n" +
                    "} ";*/


//            Tuple[] ttu = ts.sparqlSelect(q);

//            Tuple[] ttu = ts.sparqlSelectWithTextIndexing(q, new Property[]{SKOS.prefLabel}, "fr");
//            Tuple[] ttu = ts.sparqlSelectWithTextIndexing(q, new Property[]{SKOS.prefLabel}, new String[]{"fr", "en"}, null);
//            for (Tuple ssu : ttu) {
//                System.out.println("n = " + ssu);
//            }


            //ASK
            /*String q2 = "ASK  " +
                    "{" +
                    "<http://uri/res1> ?p ?o " +
                    "}";
            boolean b = ts.sparqlAsk(q2);
            System.out.println("b = " + b);*/

            //DESCRIBE
           /* String q = "DESCRIBE <http://uri/res1> ";// +
//                "FROM  <http://localhost/ns#testNamed> ";
//                    "WHERE { " +
//                    "    ?a  <http://www.w3.org/2004/02/skos/core#broadMatch> ?b " +
//                    "}";
            Triple[] res = ts.sparqlDescribe(q);
            for (Triple ss : res) {
                System.out.println("n = " + ss);
            }*/

            //*//*CONSTRUCT
            /*String q = "CONSTRUCT {" +
                        "    ?a <http://youpi/prop> ?b ." +
                        "} " +
                        "WHERE { " +
                        "    ?a  <http://www.w3.org/2004/02/skos/core#broadMatch> ?b " +
                        "}";
            Triple[] res = ts.sparqlConstruct(q);
            for (Triple ss : res) {
                System.out.println("n = " + ss);
            }*/


            //UPDATE
            /*String q = "INSERT DATA { " +
                    "    <http://uri/res2>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res3> . " +
                    "    <http://uri/res4>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res5> . " +
                    "}";
            ts.sparqlUpdate(q);*/


            /*String q = "DELETE DATA { " +
                    "    <http://uri/res2>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res3> . " +
                    "    <http://uri/res4>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res5> . " +
                    "}";
            ts.sparqlUpdate(q);*/

//            Invoker inv = new Invoker(ts,
//                    "licef.tsapi.TripleStore",
//                         "getAllTriples", new Object[]{new String[]{}});
//            Triple[] tt = (Triple[])ts.doTransaction(inv);
//            Triple[] tt = ts.getAllTriples();
            /*for (Triple ss : tt) {
                System.out.println("n = " + ss);
            }*/

           /* tt = ts.getAllTriples("voca");
            for (Triple ss : tt) {
                System.out.println("n = " + ss);
            }*/
//            ts.dump("e:/zzz/tsapiTest/output.n3", "N-TRIPLE");
//            ts.dump("e:/zzz/tsapiTest/output.ttl", "TURTLE");
//            ts.dump("e:/zzz/tsapiTest/output.rdf", "RDF/XML");
//            ts.dump("e:/zzz/tsapiTest/output.json", "RDF/JSON");
//            ts.dumpDataset("e:/zzz/tsapiTest/output.trig", Constants.TRIG);
//            ts.dumpDataset("e:/zzz/tsapiTest/output.nquads", Constants.N_QUADS);
//            ts.dumpDataset("e:/zzz/tsapiTest/output.jsonld", Constants.JSON_LD);
//            System.out.println("avant");
//            ts.loadDataset("e:/zzz/tsapiTest/output.jsonld", Constants.JSON_LD, true);
//
//            System.out.println("apres");

           /* tt = ts.getAllTriples();
            for (Triple ss : tt) {
                System.out.println("n = " + ss);
            }

            tt = ts.getAllTriples("voca");
            for (Triple ss : tt) {
                System.out.println("n = " + ss);
            }*/


            //Simili-closure
            Invoker inv = new Invoker(null, "Test", "transactionCall", new Object[]{});
            int i = 0;
            while (i < 10000 ) {
                ts.transactionalCall(inv, TripleStore.WRITE_MODE);
                Thread.sleep(600);
                i++;
            }


//            Invoker inv = new Invoker(null, "Test", "transactionCall3", new Object[]{});
//            ts.transactionalCall(inv);
//            ts.transactionalCall(inv, TripleStore.WRITE_MODE);

//            transactionCall2();

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }


    public static void launchControlPanel(final TripleStore ts) {
        JFrame frame = new JFrame();
        frame.setLayout(new FlowLayout(5));
        frame.setSize(250, 200);
        JButton buttonQuery = new JButton("Query");
        buttonQuery.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                 query(ts);
            }
        });
        JButton buttonInsert = new JButton("Insert");
        buttonInsert.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                insert(ts);
            }
        });
        JButton buttonRemoveAll = new JButton("Remove All");
        buttonRemoveAll.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                clear(ts);
            }
        });
        frame.add(buttonQuery);
        frame.add(buttonInsert);
        frame.add(buttonRemoveAll);
        frame.setVisible(true);

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    public static void query(TripleStore ts) {
        try {
            /*Triple[] triples = ts.getAllTriples();
            for (Triple triple : triples) {
                System.out.println(triple.toString());
            }*/

            String q = "PREFIX text: <http://jena.apache.org/text#> \n" +
                    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> \n" +
                    "SELECT DISTINCT ?s \n" +
                    "WHERE { \n" +
                    "    (?s ?score) text:query ( skos:prefLabel \"amis\" \"lang:fr\" ) \n" +
                    "}";

            IndexConfig cfg = new IndexConfig(indexPredicatesMain, languages, null);

            Invoker inv = new Invoker(ts, "licef.tsapi.TripleStore", "sparqlSelect_textIndex",
                    new Object[]{q, cfg});
            Tuple[] ttu = (Tuple[])ts.transactionalCall(inv);
            System.out.println("CLICK: = " + ttu[0]);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insert(TripleStore ts) {
        ArrayList<Triple> list = new ArrayList<Triple>();
        Triple t = new Triple("http://uri/book1", "dc:title", "Hamlet", true);
        list.add(t);
        t = new Triple("http://uri/book2", "dc:title", "les misérables", true);
        list.add(t);
        t = new Triple("http://uri/book2", "dc:author", "http://uri/Victor_Hugo", false);
        list.add(t);
        try {
            ts.insertTriples(list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void clear(TripleStore ts) {
        try {
            ts.clear();
            System.out.println("default graph cleared.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static Property[] indexPredicatesMain = new Property[]{SKOS.prefLabel, DCTERMS.title};
    static Property[] indexPredicatesVoc = new Property[]{SKOS.prefLabel};

    static String[] languages = new String[]{"en", "fr"};

    public static void transactionCall() throws Exception {

        String q = "PREFIX text: <http://jena.apache.org/text#> \n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> \n" +
                "SELECT DISTINCT ?s ?label \n" +
                "WHERE { \n" +
                "    (?s ?score) text:query ( skos:prefLabel \"amis\" \"lang:fr\" ) . \n" +
                " ?s skos:prefLabel ?label \n" +
                "}";


        ArrayList<Triple> l1 = new ArrayList<Triple>();
        Triple t0 = new Triple("http://ress1", SKOS.prefLabel, "ca marche, bonjour les amis "+ (new Date().getTime()), "fr");
        Triple t1 = new Triple("http://ress1", SKOS.prefLabel, "les transactions", "fr");
        l1.add(t0);
//        l1.add(t1);

        ArrayList<Triple> l2 = new ArrayList<Triple>();
        t0 = new Triple("http://resource2", DCTERMS.title, "my car", "en");
        t1 = new Triple("http://resource2", DCTERMS.title, "is broken", "en");
//        l2.add(t0);
//        l2.add(t1);

        ArrayList<Triple> l3 = new ArrayList<Triple>();
        t0 = new Triple("http://resource333", DCTERMS.title, "the film", "en");
        t1 = new Triple("http://resource333", DCTERMS.title, "is good", "en");
//        l3.add(t0);
//        l3.add(t1);

//        ts.insertTriples(l);
//        ts.removeTriples(l);
//        ts.dumpDataset("e:/zzz/tsapiTest/output.trig", Constants.TRIG);
//        int i = 0;
//        int j =  5/i;

        IndexConfig cfg = new IndexConfig(indexPredicatesMain, languages, null);
        ts.insertTriples_textIndex(l1, cfg);

//        ts.insertTriples_textIndex(l2, cfg);
//        int i = 0;
//        i =  5/i;

//        ts.insertTriples_textIndex(l3, cfg);

//        ts.insertTriples_textIndex(l1, cfg);
//        ts.insertTriples_textIndex(l2, cfg);
//        ts.insertTriples_textIndex(l3, cfg);


//        ts.insertTriplesWithTextIndexing(l1, indexPredicatesMain, "en", null);
//        ts.interruptTransaction();
//        int i = 0;
//        i = i / 0;
//        ts.insertTriplesWithTextIndexing(l2, indexPredicates, "en", null);
//        ts.removeTriplesWithTextIndexing(l1, indexPredicates, "en", null);
//        ts.removeTriplesWithTextIndexing(l2, indexPredicates, "en", null);

//        int i = 0;
//        int j =  5/i;
//        ts.interruptTransaction();


        IndexConfig cfg2 = new IndexConfig(new Property[]{SKOS.prefLabel}, new String[]{"fr", "en"}, null);
//        Tuple[] ttu = ts.sparqlSelect(q);

        Tuple[] ttu = ts.sparqlSelect_textIndex(q, cfg);
        if (ttu.length > 0)
            System.out.println("results loop: = " + ttu[0] );

//        ts.dumpDataset("e:/zzz/tsapiTest/output.trig", Constants.TRIG);
    }


    public static void transactionCall2() throws Exception {
        System.out.println("\n\nTRANSAC 2");
        String q = "PREFIX text: <http://jena.apache.org/text#> \n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> \n" +
                "SELECT * \n" +
                "WHERE { \n" +
                "?uri text:query ( skos:prefLabel \"salut\" \"lang:fr\" ) .\n" +
                "?uri skos:prefLabel ?label .\n" +
                "} ";

        ArrayList<Triple> l1 = new ArrayList<Triple>();
        Triple t0 = new Triple("http://res1", SKOS.prefLabel, "salut les amis", "fr-CA");
        Triple t1 = new Triple("http://res2", SKOS.prefLabel, "j'invite des amis", "fra");
        Triple t2 = new Triple("http://res3", SKOS.prefLabel, "chaussure");
        l1.add(t0);
        l1.add(t1);
        l1.add(t2);

//        ts.insertTriples(l1);
//        ts.insertTriplesWithTextIndex(l1, indexPredicatesVoc, languages, null);

//        Tuple[] ttu = ts.sparqlSelect(q);

        IndexConfig cfg = new IndexConfig(indexPredicatesVoc, languages, null);
        Tuple[] ttu = ts.sparqlSelect_textIndex(q, cfg);
        System.out.println("results: = " );
        for (Tuple ssu : ttu) {
            System.out.println("n = " + ssu);
        }



    }


    public static void transactionCall3() throws Exception {
        System.out.println("\n\nTRANSAC 3");
        ArrayList<Triple> l1 = new ArrayList<Triple>();
        Triple t0 = new Triple("http://res1", "http://pred1", "http://res2", false);
        Triple t1 = new Triple("http://res10", "http://pred1", "http://res20", false);
        Triple t2 = new Triple("http://res100", "http://pred1", "http://res200", false);
        l1.add(t0);
        l1.add(t1);
        l1.add(t2);

//        ts.insertTriples(l1);
//
        ts.removeTriple(t1);
        ts.removeTriple(t2);

        Triple[] res = ts.getAllTriples();
        for (Triple t : res) {
            System.out.println("t = " + t);
        }

        Tuple[] tuples = ts.sparqlSelect("select * where { ?s ?p ?o }");
        for (Tuple t : tuples) {
            System.out.println("tuple = " + t);
        }

    }
}
