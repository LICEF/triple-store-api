package licef.tsapi.model;

import com.hp.hpl.jena.ontology.ObjectProperty;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.rdf.model.Property;
import licef.LangUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

public class Triple {

    public Triple() {
    }

    public Triple(String subject, String predicate, String object, boolean isObjectLiteral) {
        this(subject, predicate, object, isObjectLiteral, null);
    }

    public Triple(String subject, Property predicate, OntClass object) {
        this(subject, predicate.getURI(), object.getURI(), false, null);
    }

    public Triple(String subject, Property predicate, String object) {
        this(subject, predicate.getURI(), object, !(predicate instanceof ObjectProperty), null);
    }

    public Triple(String subject, Property predicate, String object, String language) {
        this(subject, predicate.getURI(), object, !(predicate instanceof ObjectProperty), language);
    }

    public Triple(String subject, String predicate, String object, boolean isObjectLiteral, String language) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.isObjectLiteral = isObjectLiteral;
        if ("".equals(language))
            language = null;
        this.language = LangUtil.convertLangToISO2( language );
    }

    public String getSubject() {
        return( subject );
    }

    public void setSubject( String subject ) {
        this.subject = subject;
    }

    public String getPredicate() {
        return( predicate );
    }

    public void setPredicate( String predicate ) {
        this.predicate = predicate;
    }

    public String getObject() {
        return( object );
    }

    public void setObject( String object ) {
        this.object = object;
    }

    public boolean isEmptyObject() {
        return( object == null || "".equals( object.trim() ) );
    }

    public boolean isObjectLiteral() {
        return isObjectLiteral;
    }

    public void setLiteral(boolean literal) {
        isObjectLiteral = literal;
    }

    public String getLanguage() {
        return( language );
    }

    public void setLanguage( String language ) {
        this.language = LangUtil.convertLangToISO2( language );
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("<").append(subject).append("> ");
        str.append("<").append(predicate).append("> ");
        if (isObjectLiteral()) {
            str.append("\"").append(object).append("\"");
            if (getLanguage() != null)
                str.append( "@" ).append(getLanguage());
        }
        else
            str.append("<").append(object).append(">");
        return( str.toString() );
    }

    public boolean equals( Object obj ) {
        if( obj == null )
            return( false );
        if( !( obj instanceof Triple ) )
            return( false );
        Triple triple = (Triple)obj;
        if( ( isObjectLiteral() && !triple.isObjectLiteral() ) || ( !isObjectLiteral() && triple.isObjectLiteral() ) )
            return( false );
        if( getSubject() == null && triple.getSubject() != null )
            return( false );
        if( !getSubject().equals( triple.getSubject() ) )
            return( false );
        if( getPredicate() == null && triple.getPredicate() != null )
            return( false );
        if( !getPredicate().equals( triple.getPredicate() ) )
            return( false );
        if( getObject() == null && triple.getObject() != null )
            return( false );
        if( !getObject().equals( triple.getObject() ) )
            return( false );
        if( !getLanguage().equals( triple.getLanguage() ) )
            return( false );
        return( true );
    }

    public int hashCode() {
        return( ( subject + predicate + object + isObjectLiteral + language ).hashCode() );
    }

    public static Triple[] readTriplesFromXml( String xml ) throws JSONException {
        JSONObject json = XML.toJSONObject( xml );
        return( readTriplesFromJson( json.toString() ) );
    }

    public static Triple[] readTriplesFromJson( String strJson ) throws JSONException {
        JSONObject json = new JSONObject( strJson );
        Triple[] triples;
        try {
            JSONArray array = json.getJSONObject( "triples" ).getJSONArray( "triple" );
            triples = new Triple[ array.length() ];
            for( int i = 0; i < array.length(); i++ ) {
                JSONObject object = array.getJSONObject( i );
                triples[ i ] = readTripleFromJson(object);
            }
        } catch (JSONException e) {
            JSONObject object = json.getJSONObject( "triples" ).getJSONObject( "triple" );
            Triple triple = readTripleFromJson(object);
            triples = new Triple[]{triple};
        }
        return( triples );
    }

    public static Triple readTripleFromJson( JSONObject jsonTriple) throws JSONException {
        String subject = jsonTriple.get( "subject" ).toString();
        String predicate = jsonTriple.get( "predicate" ).toString();
        String object = jsonTriple.get( "object" ).toString();
        String strIsLiteral = jsonTriple.get( "literal" ).toString();
        boolean isLiteral = "true".equals( strIsLiteral );
        String language = ( jsonTriple.has( "language" ) ? jsonTriple.get( "language" ).toString() : null );
        return new Triple( subject, predicate, object, isLiteral, language );
    }
    
    public static String writeTriplesToXml( Triple[] triples ) throws JSONException {
        JSONObject json = writeTriplesToJSONObject( triples );
        return( XML.toString( json ) );
    }

    public static String writeTriplesToJson( Triple[] triples ) throws JSONException {
        JSONObject json = writeTriplesToJSONObject( triples );
        return( json.toString() );
    }

    private static JSONObject writeTriplesToJSONObject( Triple[] triples ) throws JSONException {
        JSONObject json = new JSONObject();
        JSONArray jsonTriples = new JSONArray();
        for( int i = 0; i < triples.length; i++ ) {
            JSONObject jsonTriple = new JSONObject();
            jsonTriple.put( "subject", triples[ i ].getSubject() );
            jsonTriple.put( "predicate", triples[ i ].getPredicate() );
            jsonTriple.put( "object", triples[ i ].getObject() );
            jsonTriple.put( "literal", triples[ i ].isObjectLiteral() ? "true" : "false" );
            if( triples[ i ].getLanguage() != null )
                jsonTriple.put( "language", triples[ i ].getLanguage() );
            jsonTriples.put( jsonTriple );
        }
        json.put( "triple", jsonTriples );
        return( json );
    }

    private String subject;
    private String predicate;
    private String object;
    private boolean isObjectLiteral;
    private String language;

}
