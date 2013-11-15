import licef.reflection.Invoker;
import licef.reflection.ThreadInvoker;
import licef.tsapi.Triple;
import licef.tsapi.TripleStore;

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
        String dbPath = "databaseTest/db1";
        TripleStore ts = new TripleStore(dbPath);
        ts.start();

        launchControlPanel(ts);
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
        ts.listTriples();
    }

    public static void insert(TripleStore ts) {
        ArrayList<Triple> list = new ArrayList<Triple>();
        Triple t = new Triple("http://uri/book1", "dc:title", "Hamlet", true);
        list.add(t);
        t = new Triple("http://uri/book2", "dc:title", "les misérables", true);
        list.add(t);
        t = new Triple("http://uri/book2", "dc:author", "http://uri/Victor_Hugo", false);
        list.add(t);
        ts.insertTriples(list);
    }

    public static void clear(TripleStore ts) {
        ts.clear();
    }


}
