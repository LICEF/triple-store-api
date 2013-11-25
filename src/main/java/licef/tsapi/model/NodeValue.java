package licef.tsapi.model;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-20
 */
public class NodeValue {

    public NodeValue() {
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isLiteral() {
        return isLiteral;
    }

    public void setLiteral(boolean literal) {
        isLiteral = literal;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
        setLiteral(true);
    }

    public String toString() {
        String res = value;
        if (isLiteral()) {
            res = "\"" + res + "\"";
            if (language != null && !"".equals(language))
                res += "@" + language;
        }
        else
            res = "<" + res + ">";
        return res;
    }

    String value;
    boolean isLiteral = false;
    String language;
}
