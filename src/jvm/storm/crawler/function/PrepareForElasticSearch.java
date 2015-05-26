package storm.crawler.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import storm.crawler.CrawlerConfig;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class PrepareForElasticSearch extends BaseFunction {

    private String esIndex;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.esIndex = conf.get(CrawlerConfig.ELASTICSEARCH_INDEX_NAME).toString();
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String url = tridentTuple.getString(0);
        String content_html = tridentTuple.getString(1);
        String title = tridentTuple.getString(2);

        JSONObject jsonSource = new JSONObject();
        jsonSource.put("url", url);
        jsonSource.put("content", content_html);
        jsonSource.put("title", title);

        tridentCollector.emit(new Values(esIndex, "crawl_type", JSONObject.escape(url) , jsonSource.toJSONString() ));
    }
}
