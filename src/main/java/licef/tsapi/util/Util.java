package licef.tsapi.util;

import com.hp.hpl.jena.ontology.ObjectProperty;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import licef.StringUtil;
import licef.tsapi.vocabulary.*;

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
        vocabularies.put(DCTERMS.getURI(), DCTERMS.class);
        vocabularies.put(SKOS.getURI(), SKOS.class);
        vocabularies.put(VCARD.getURI(), VCARD.class);
        vocabularies.put(FOAF.getURI(), FOAF.class);
    }

    public static void registerVocabulary(String vocUri, Class vocClass) {
        vocabularies.put(vocUri, vocClass);

        System.out.println("vocabularies = " + vocabularies);
    }

    public static Class getVocabulary(String uri) {
        Resource res = ResourceFactory.createResource(uri);
        String ns = res.getNameSpace();
        return vocabularies.get(ns);
    }

    public static String getIndexFieldProperty(Property property) {
        Class vocClass = getVocabulary(property.getURI());
        String[] nameSplitted = StringUtil.split(vocClass.getName(), '.');
        String className = nameSplitted[nameSplitted.length - 1].toLowerCase();
        return className + "-" + property.getLocalName();
    }

}
