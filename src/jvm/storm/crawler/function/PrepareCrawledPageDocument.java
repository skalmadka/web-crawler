package storm.crawler.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class PrepareCrawledPageDocument extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String url = JSONObject.escape(tridentTuple.getString(0));
        String content_html = JSONObject.escape(tridentTuple.getString(1));
        String title = JSONObject.escape(tridentTuple.getString(2));


        String source = "{\"url\":\""+url+"\", \"content\":\""+content_html+"\", \"title\":\""+title+"\"}";
//        System.out.println("----- PrepareCrawledPageDocument: id = "+url);
//        System.out.println("----- PrepareCrawledPageDocument: source = "+source);

        tridentCollector.emit(new Values("crawl_index", "crawl_type", url, source ));
    }
}
