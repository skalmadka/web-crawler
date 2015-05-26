package storm.crawler.function;

import backtype.storm.tuple.Values;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.index.query.QueryBuilders;
import storm.crawler.CrawlerConfig;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by Sunil Kalmadka on 5/1/15.
 */
public class PrepareSearchQuery  extends BaseFunction {

    private String esIndex;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.esIndex = conf.get(CrawlerConfig.ELASTICSEARCH_INDEX_NAME).toString();
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String queryString = tridentTuple.getString(0);

        String esQuery = QueryBuilders.matchQuery("content", queryString).buildAsBytes().toUtf8();
        tridentCollector.emit(new Values(esQuery,  Lists.newArrayList(esIndex), Lists.newArrayList("crawl_type")) );
    }
}