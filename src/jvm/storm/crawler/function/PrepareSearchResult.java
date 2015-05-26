package storm.crawler.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 5/4/15.
 */
public class PrepareSearchResult  extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String resultsJSON = tridentTuple.getString(0);


        tridentCollector.emit(new Values(resultsJSON) );
    }
}