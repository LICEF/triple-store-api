package licef.tsapi.model;

import java.util.Hashtable;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-20
 */
public class Tuple {

    public Tuple() {
        values = new Hashtable<String, Node>();
    }

    public Tuple(String[] varNames) {
        this();
        this.varNames = varNames;
    }

    public void setVarNames(String[] varNames) {
        this.varNames = varNames;
    }

    public String[] getVarNames() {
        return varNames;
    }

    public Node getValue(String var) {
        return values.get(var);
    }

    public void setValue(String var, Node node) {
        values.put(var, node);
    }

    public String toString() {
        return values.toString();
    }

    String[] varNames;
    Hashtable<String, Node> values;
}
