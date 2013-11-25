package licef.tsapi.vocabulary;
/* CVS $Id: $ */
 
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.ontology.*;
 
/**
 * Vocabulary definitions from skos.rdf 
 * @author Auto-generated by schemagen on 19 nov. 2013 11:40 
 */
public class SKOS {
    /** <p>The ontology model that holds the vocabulary terms</p> */
    private static OntModel m_model = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, null);

    /** <p>The namespace of the vocabulary as a string</p> */
    public static final String NS = "http://www.w3.org/2004/02/skos/core#";
    
    /** <p>The namespace of the vocabulary as a string</p>
     *  @see #NS */
    public static String getURI() {return NS;}
    
    /** <p>The namespace of the vocabulary as a resource</p> */
    public static final Resource NAMESPACE = m_model.createResource( NS );
    
    public static final ObjectProperty broadMatch = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#broadMatch");
    
    /** <p>Broader concepts are typically rendered as parents in a concept hierarchy 
     *  (tree).</p>
     */
    public static final ObjectProperty broader = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#broader");
    
    public static final ObjectProperty broaderTransitive = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#broaderTransitive");
    
    public static final ObjectProperty closeMatch = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#closeMatch");
    
    /** <p>skos:exactMatch is disjoint with each of the properties skos:broadMatch and 
     *  skos:relatedMatch.</p>
     */
    public static final ObjectProperty exactMatch = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#exactMatch");
    
    public static final ObjectProperty hasTopConcept = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#hasTopConcept");
    
    public static final ObjectProperty inScheme = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#inScheme");
    
    /** <p>These concept mapping relations mirror semantic relations, and the data model 
     *  defined below is similar (with the exception of skos:exactMatch) to the data 
     *  model defined for semantic relations. A distinct vocabulary is provided for 
     *  concept mapping relations, to provide a convenient way to differentiate links 
     *  within a concept scheme from links between concept schemes. However, this 
     *  pattern of usage is not a formal requirement of the SKOS data model, and relies 
     *  on informal definitions of best practice.</p>
     */
    public static final ObjectProperty mappingRelation = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#mappingRelation");
    
    public static final ObjectProperty member = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#member");
    
    /** <p>For any resource, every item in the list given as the value of the skos:memberList 
     *  property is also a value of the skos:member property.</p>
     */
    public static final ObjectProperty memberList = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#memberList");
    
    public static final ObjectProperty narrowMatch = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#narrowMatch");
    
    /** <p>Narrower concepts are typically rendered as children in a concept hierarchy 
     *  (tree).</p>
     */
    public static final ObjectProperty narrower = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#narrower");
    
    public static final ObjectProperty narrowerTransitive = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#narrowerTransitive");
    
    /** <p>skos:related is disjoint with skos:broaderTransitive</p> */
    public static final ObjectProperty related = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#related");
    
    public static final ObjectProperty relatedMatch = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#relatedMatch");
    
    public static final ObjectProperty semanticRelation = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#semanticRelation");
    
    public static final ObjectProperty topConceptOf = m_model.createObjectProperty("http://www.w3.org/2004/02/skos/core#topConceptOf");
    
    public static final DatatypeProperty notation = m_model.createDatatypeProperty("http://www.w3.org/2004/02/skos/core#notation");
    
    /** <p>The range of skos:altLabel is the class of RDF plain literals.skos:prefLabel, 
     *  skos:altLabel and skos:hiddenLabel are pairwise disjoint properties.</p>
     */
    public static final AnnotationProperty altLabel = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#altLabel");
    
    public static final AnnotationProperty changeNote = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#changeNote");
    
    public static final AnnotationProperty definition = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#definition");
    
    public static final AnnotationProperty editorialNote = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#editorialNote");
    
    public static final AnnotationProperty example = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#example");
    
    /** <p>The range of skos:hiddenLabel is the class of RDF plain literals.skos:prefLabel, 
     *  skos:altLabel and skos:hiddenLabel are pairwise disjoint properties.</p>
     */
    public static final AnnotationProperty hiddenLabel = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#hiddenLabel");
    
    public static final AnnotationProperty historyNote = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#historyNote");
    
    public static final AnnotationProperty note = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#note");
    
    /** <p>skos:prefLabel, skos:altLabel and skos:hiddenLabel are pairwise disjoint properties.A 
     *  resource has no more than one value of skos:prefLabel per language tag, and 
     *  no more than one value of skos:prefLabel without language tag.The range of 
     *  skos:prefLabel is the class of RDF plain literals.</p>
     */
    public static final AnnotationProperty prefLabel = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#prefLabel");
    
    public static final AnnotationProperty scopeNote = m_model.createAnnotationProperty("http://www.w3.org/2004/02/skos/core#scopeNote");
    
    public static final OntClass Collection = m_model.createClass("http://www.w3.org/2004/02/skos/core#Collection");
    
    public static final OntClass Concept = m_model.createClass("http://www.w3.org/2004/02/skos/core#Concept");
    
    public static final OntClass ConceptScheme = m_model.createClass("http://www.w3.org/2004/02/skos/core#ConceptScheme");
    
    public static final OntClass OrderedCollection = m_model.createClass("http://www.w3.org/2004/02/skos/core#OrderedCollection");
}
