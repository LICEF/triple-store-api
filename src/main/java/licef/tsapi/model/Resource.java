package licef.tsapi.model;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-11-20
 */
public class Resource extends Node {

    public Resource(String uri) {
        this.uri = uri;
    }

    public String toString() {
        return "<" + uri + ">";
    }

    String uri;
}
