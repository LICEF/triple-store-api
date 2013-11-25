package licef.tsapi.vocabulary;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.Property;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-19
 */
public abstract class VocabularyModel {

    protected static OntModel m_model;

    public static Property getProperty(String prop) {
        return m_model.getProperty(prop);
    }

    //public abstract String getVocabularyName();
}
