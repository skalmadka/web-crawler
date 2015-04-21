package storm.crawler.function;


import backtype.storm.tuple.Values;
import storm.crawler.common.Readability;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import java.io.IOException;
import java.net.URL;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class GetAdFreeWebPage  extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String str = tridentTuple.getString(0);
        String[] strSplit = str.split(" ");

        String url = strSplit[0];
        int depth = 0;
        if(strSplit.length > 1)
            depth = Integer.parseInt(strSplit[1]);

        Readability readability = null;
        Integer timeoutMillis = 4000;

        try {
            readability = new Readability(new URL(url), timeoutMillis);  // URL
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        readability.init();

        String webPageString = readability.content; //readability.outerHtml();
        String webPageTitle = readability.title;
        String hrefString = readability.hrefString.toString();

        System.out.println("GetAdFreeWebPage: hrefString: \""+ hrefString+"\"");
        tridentCollector.emit(new Values(url, webPageString, webPageTitle, hrefString, Integer.toString(depth)));
    }
}
