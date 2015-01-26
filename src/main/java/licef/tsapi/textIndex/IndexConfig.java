package licef.tsapi.textIndex;

import com.hp.hpl.jena.rdf.model.Property;

/**
 * Created by amiara on 2015-01-26.
 */
public class IndexConfig {

    private Property[] predicatesToIndex;
    private Object langInfo;
    private String indexName;

    /**
      * @param predicatesToIndex array of properties to index
      * @param langInfo formats are :
      *                 - null for standard index
      *                 - langstring for localized index (ex: "fr")
      *                 - array of langstrings for multi-lingual index (ex: ["fr", "en"])
      * @param indexName if not null, separate lucene index is used with name indexName, "default" otherwise
      */

    public IndexConfig(Property[] predicatesToIndex, Object langInfo, String indexName) {
        this.predicatesToIndex = predicatesToIndex;
        this.langInfo = langInfo;
        this.indexName = indexName;
    }

    public Property[] getPredicatesToIndex() {
        return predicatesToIndex;
    }

    public Object getLangInfo() {
        return langInfo;
    }

    public String getIndexName() {
        return indexName;
    }
}
