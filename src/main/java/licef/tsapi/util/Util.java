package licef.tsapi.util;

import licef.tsapi.Constants;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-19
 */
public class Util {

    public static String getFormatName(int format) {
        switch (format) {
            case Constants.RDFXML:   return "RDF/XML";
            case Constants.TURTLE:   return "TURTLE";
            case Constants.N_TRIPLE: return "N-TRIPLE";
            case Constants.JSON:     return "RDF/JSON";
            case Constants.HTML:     return "HTML";
            case Constants.XHTML:    return "XHTML";
            case Constants.RDFA:     return "RDFA";
            case Constants.JSON_LD:  return "JSON-LD";
            case Constants.N_QUADS:  return "N-QUADS";
            case Constants.TRIG:     return "TRIG";
        }
        return null;
    }
}
