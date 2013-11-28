import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.tdb.TDBFactory;
import licef.tsapi.model.NodeValue;
import licef.tsapi.model.Triple;
import licef.tsapi.TripleStore;
import licef.tsapi.model.Tuple;
import licef.tsapi.util.Util;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-15
 */
public class Test {

    public static void main(String[] args) {
        try {
        String dbPath = "e:/zzz/tsapiTest/data";
//            String dbPath = "e:/proeaf/database";
            String serverPath = "e:/zzz/tsapiTest";
            TripleStore ts = new TripleStore(dbPath, serverPath, null);
//        ts.startServer();


//            ts.loadContent(new FileInputStream("e:/zzz/tsapiTest/output.ttl"), TripleStore.TURTLE, "vv");
//            ts.loadContent(new FileInputStream("e:/zzz/tsapiTest/output.n3"), TripleStore.N_TRIPLE);
//            ts.loadRDFa(new URL("http://www.3kbo.com/examples/rdfa/simple.html").openStream(), "http://local1");
//            ts.loadRDFa(new URL("http://examples.tobyinkster.co.uk/hcard").openStream(), "http://local2");
//            ts.loadRDFa(new FileInputStream("e:/zzz/tsapiTest/big.htm"), "http://local3");


//        launchControlPanel(ts);

/*
            ArrayList<Triple> l = new ArrayList<Triple>();
            Triple t1 = new Triple("http://uri/res 10", SKOS.broadMatch, "http://uri/res2");
            Triple t2 = new Triple("http://uri/res1", SKOS.prefLabel, "Hello");
            Triple t3 = new Triple("http://uri/res1", SKOS.prefLabel, "bonjour les amis", "fr");
            Triple t4 = new Triple("http://uri/res2", SKOS.prefLabel, "comment allez vous", "fr");
            Triple t5 = new Triple("http://uri/res2", SKOS.broadMatch, "http://uri/res3");
            l.add(t1);
            l.add(t2);
            l.add(t3);
            l.add(t4);
            l.add(t5);
*/


//            ts.insertTriples(l);
//            ts.removeTriplesWithTextIndexing(l, new Property[]{SKOS.prefLabel}, new String[]{"fr", "en"});
//            ts.insertTriplesWithTextIndexing(l, new Property[]{SKOS.prefLabel}, null);
//            ts.insertTriplesWithTextIndexing(l, new Property[]{SKOS.prefLabel}, new String[]{"fr", "en"});
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


            Triple[] tt = ts.getAllTriples();
            for (Triple ss : tt) {
                System.out.println("n = " + ss);
            }
//            ts.dump("e:/zzz/tsapiTest/output.n3", "N-TRIPLE");
//            ts.dump("e:/zzz/tsapiTest/output.ttl", "TURTLE");
//            ts.dump("e:/zzz/tsapiTest/output.rdf", "RDF/XML");
//            ts.dump("e:/zzz/tsapiTest/output.json", "RDF/JSON");
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
            @Override
            public void actionPerformed(ActionEvent e) {
                 query(ts);
            }
        });
        JButton buttonInsert = new JButton("Insert");
        buttonInsert.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                insert(ts);
            }
        });
        JButton buttonRemoveAll = new JButton("Remove All");
        buttonRemoveAll.addActionListener(new ActionListener() {
            @Override
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
            Triple[] triples = ts.getAllTriples();
            for (Triple triple : triples) {
                System.out.println(triple.toString());
            }
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


}
