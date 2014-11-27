package licef.tsapi.util;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created with IntelliJ IDEA.
 * User: amiara
 * Date: 13-12-06
 */
public class Translator {

    public static Model loadContent(InputStream is, String baseUri, int format) {
        Model modelTmp = ModelFactory.createDefaultModel();
        modelTmp.read(is, baseUri, Util.getFormatName(format));
        return modelTmp;
    }

    public static void translate(Model model, int format, OutputStream os) throws Exception {
        RDFDataMgr.write(os, model, RDFLanguages.nameToLang(Util.getFormatName(format)));
    }

    public static Dataset loadDataset(String path, int format) {
        return RDFDataMgr.loadDataset(path, RDFLanguages.nameToLang(Util.getFormatName(format)));
    }

    public static void translate(Dataset dataset, int format, OutputStream os) throws Exception {
        RDFDataMgr.write(os, dataset, RDFLanguages.nameToLang(Util.getFormatName(format)));
    }

    public static void convert(InputStream is, String baseUri, int fromFormat,
                               int toFormat, OutputStream os) throws Exception {
        Model model = loadContent(is, baseUri, fromFormat);
        translate(model, toFormat, os);
    }

}
