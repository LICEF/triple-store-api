package licef.tsapi.model;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-20
 */
public class Literal extends Node {

    public Literal(String value, String language) {
        this.value = value;
        this.language = language;
    }

    public String toString() {
        String res = "\"" + value + "\"";
        if (language != null && !"".equals(language))
            res += "@" + language;
        return res;
    }

    String value;
    String language;
}
