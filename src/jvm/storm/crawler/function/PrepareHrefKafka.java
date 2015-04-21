package storm.crawler.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class PrepareHrefKafka  extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String hrefList = tridentTuple.getString(0);
        Integer depth = Integer.parseInt(tridentTuple.getString(1));

        if(hrefList == null || hrefList.trim().length() == 0 || depth == 0)
            return;
        depth--;

//        System.out.println("PrepareHrefKafka: \""+hrefList+"\"");

        String[] hrefArray = hrefList.split(" ");
        for(String href : hrefArray) {
            tridentCollector.emit(new Values(href, depth.toString()));
        }
    }
}