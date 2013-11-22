import licef.tsapi.model.Triple;
import licef.tsapi.TripleStore;
import licef.tsapi.model.Tuple;
import licef.tsapi.util.Util;
import licef.tsapi.vocabulary.FOAF;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-15
 */
public class Test {

    public static void main(String[] args) {
        String dbPath = "e:/zzz/tsapiTest/data";
        String serverPath = "e:/zzz/tsapiTest";
        TripleStore ts = new TripleStore(dbPath, serverPath, null);

        String b = Util.getIndexFieldProperty(FOAF.firstName);
        System.out.println("b = " + b);
        /*ts.startServer();
        try {
            ts.loadRdf("e:/zzz/C.rdf");
        } catch (Exception e) {
            e.printStackTrace();
        }
*/
//
//        launchControlPanel(ts);

        /*ArrayList<Triple> l = new ArrayList<Triple>();
        Triple t1 = new Triple("http://uri/res1", SKOS.broadMatch, "http://uri/res2");
        Triple t2 = new Triple("http://uri/res1", SKOS.prefLabel, "Hello");
        Triple t3 = new Triple("http://uri/res1", SKOS.altLabel, "bonjour les amis", "fr");
        l.add(t1);
        l.add(t2);
        l.add(t3);*/

//        ts.insertTriples(l);
       /* Triple[] tt = ts.getAllTriples();
        for (Triple ss : tt) {
            System.out.println("n = " + ss);
        }*/



        /*//SELECT
        String q = "SELECT ?e ?d ?k WHERE { ?e ?d ?k . } ";
        Tuple[] ttu = ts.sparqlSelect(q);
        for (Tuple ssu : ttu) {
            System.out.println("n = " + ssu);
        }*/

        /*//ASK
        String q = "ASK { " +
                "?s <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res2> . " +
                "?b <http://www.w3.org/2004/02/skos/core#gg> ?n . " +

                "}";
        boolean b = ts.sparqlAsk(q);
        System.out.println("b = " + b);*/

        //DESCRIBE
        String q = "DESCRIBE <http://uri/res1> ";
//                    "WHERE { " +
//                    "    ?a  <http://www.w3.org/2004/02/skos/core#broadMatch> ?b " +
//                    "}";
        try {
            Triple[] res = ts.sparqlDescribe(q);
            for (Triple ss : res) {
                System.out.println("n = " + ss);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*//CONSTRUCT
        String q = "CONSTRUCT {" +
                    "    ?a <http://youpi/prop> ?b ." +
                    "} " +
                    "WHERE { " +
                    "    ?a  <http://www.w3.org/2004/02/skos/core#broadMatch> ?b " +
                    "}";
        Triple[] res = ts.sparqlConstruct(q);
        for (Triple ss : res) {
            System.out.println("n = " + ss);
        }*/



      /*//UPDATE
        q = "INSERT DATA { " +
                "    <http://uri/res2>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res3> . " +
                "    <http://uri/res4>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res5> . " +
                "}";
        ts.sparqlUpdate(q);*/


        /* q = "DELETE DATA { " +
                "    <http://uri/res2>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res3> . " +
                "    <http://uri/res4>  <http://www.w3.org/2004/02/skos/core#broadMatch> <http://uri/res5> . " +
                "}";
        ts.sparqlUpdate(q);*/
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
//        ts.listTriples();
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
