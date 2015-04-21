package storm.crawler.filter;


import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class PrintFilter  extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        System.out.println("PrintFilter: "+tridentTuple);
        return true;
    }
}

