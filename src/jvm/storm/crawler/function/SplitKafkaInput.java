package storm.crawler.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class SplitKafkaInput  extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String kafkaInput = tridentTuple.getString(0);

        String url;
        Integer depth;

        String[] tokens = kafkaInput.split(" ");
        if(tokens.length == 2){
            url = tokens[0];
            depth = Integer.parseInt(tokens[1]);
        } else if (tokens.length == 1){
            url = tokens[0];
            depth = new Integer(0);
        } else {
            return;
        }
        tridentCollector.emit(new Values(url, depth) );
    }
}