package licef.tsapi;

import com.hp.hpl.jena.tdb.TDBFactory;
import licef.IOUtil;
import licef.reflection.Invoker;
import licef.reflection.ThreadInvoker;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.FusekiCmd;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-21
 */
public class Server {

    String databasePath;
    String pagesDir;

    public Server(String databasePath, String serverDir) {
        this.databasePath = databasePath;
        this.pagesDir = serverDir + "/pages";
    }

    void start() {
        TDBFactory.createDataset(databasePath);
        initPagesDir();
        (new ThreadInvoker(new Invoker(this, "licef.tsapi.Server",
                "startFuseki", new Object[]{}))).start();
    }

    private void initPagesDir() {
        if (new File(pagesDir).exists())
            return;

        IOUtil.createDirectory(pagesDir);
        String[] files = new String[] {
                "books.ttl", "control-panel.tpl", "data-validator.html", "favicon.ico",
                "fuseki.css", "fuseki.html", "iri-validator.html", "ping.txt",
                "query-validator.html", "robots.txt", "sparql.html", "sparql.tpl",
                "update-validator.html", "xml-to-html.xsl", "xml-to-html-links.xsl", "xml-to-html-plain.xsl" };
        try {
            for (String filename : files) {
                InputStream is = this.getClass().getResourceAsStream("/pages/" + filename);
                IOUtil.copy(is, new FileOutputStream(pagesDir + "/" + filename));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void startFuseki() {
        System.out.println("Start Fuseki server...");
        Fuseki.init() ;
        new FusekiCmd("--loc=" + databasePath, "--pages=" + pagesDir, "/ds").mainRun() ;
    }

}
