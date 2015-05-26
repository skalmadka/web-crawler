package storm.crawler.function;

import backtype.storm.tuple.Values;
import org.apache.storm.http.HttpEntity;
import org.apache.storm.http.client.methods.CloseableHttpResponse;
import org.apache.storm.http.client.methods.HttpGet;
import org.apache.storm.http.impl.client.CloseableHttpClient;
import org.apache.storm.http.impl.client.HttpClients;
import org.apache.storm.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Sunil Kalmadka on 4/30/15.
 */
public class BingAutoSuggest  extends BaseFunction {
    final private int suggestionsLimit;

    public BingAutoSuggest(int suggestionsLimit){
        this.suggestionsLimit = suggestionsLimit;
    }


    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String searchQuery = tridentTuple.getString(0);

        try {
            tridentCollector.emit(new Values(searchQuery));//Emit!!! Query itself
            if(suggestionsLimit >0)
                bingAutoSuggestEmitter(searchQuery, tridentCollector);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void bingAutoSuggestEmitter(String searchQuery, TridentCollector tridentCollector) throws IOException, Exception {
        final String bingAutoSuggestURL = "http://api.bing.com/osjson.aspx?query=";

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(bingAutoSuggestURL + searchQuery);
        CloseableHttpResponse response1 = httpclient.execute(httpGet);
        try {
            System.out.println(response1.getStatusLine());
            HttpEntity entity1 = response1.getEntity();

            BufferedReader rd = new BufferedReader(new InputStreamReader(entity1.getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            JSONParser parser=new JSONParser();
            JSONArray array = (JSONArray) parser.parse(result.toString());

            JSONArray expandedQueryArray = (JSONArray) array.get(1);
            //System.out.println("~"+expandedQueryArray);

            for(int i =0; i< expandedQueryArray.size() && i< suggestionsLimit -1; i++){
                //System.out.println("`````i="+i +"\t"+ expandedQueryArray.get(i));
                tridentCollector.emit(new Values(expandedQueryArray.get(i)));//Emit!!! Emit Bing auto suggest term
            }

            EntityUtils.consume(entity1);
        } finally {
            response1.close();
        }
    }
}


