package licef.tsapi.util;

import com.hp.hpl.jena.ontology.ObjectProperty;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.RDFS;
import licef.tsapi.vocabulary.SKOS;

import java.lang.reflect.Method;
import java.util.Hashtable;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-19
 */
public class Util {

    static Hashtable<String, Class> vocabularies = new Hashtable<String, Class>();

    static {
        initVocabularies();
    }

    static void initVocabularies() {
        vocabularies.put(RDFS.getURI(), RDFS.class);
        vocabularies.put(FOAF.getURI(), FOAF.class);
        vocabularies.put(SKOS.getURI(), SKOS.class);
    }

    public static Property getProperty(String uri) {
        Resource res = ResourceFactory.createResource(uri);
        String ns = res.getNameSpace();
        Class vocClass = vocabularies.get(ns);
        try {
            Method m = vocClass.getMethod("getProperty", String.class);
            return (Property)m.invoke(null, uri);
        } catch (Exception e) {
        }
        return null;
    }

    public static Boolean isLiteralProperty(String uri) {
        Property p = getProperty(uri);
        return p != null && !(p instanceof ObjectProperty);
    }
}
