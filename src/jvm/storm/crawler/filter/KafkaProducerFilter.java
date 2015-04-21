package storm.crawler.filter;


import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import storm.crawler.CrawlerConfig;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class KafkaProducerFilter   extends BaseFilter {
    kafka.javaapi.producer.Producer<String, String> producer;
    String kafkaTopic;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        Properties props = new Properties();
        props.put("metadata.broker.list", conf.get(CrawlerConfig.KAFKA_PRODUCER_HOST_NAME)+":"+conf.get( CrawlerConfig.KAFKA_PRODUCER_HOST_PORT) );
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        this.producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
        this.kafkaTopic = (String) conf.get( CrawlerConfig.KAFKA_TOPIC_NAME );
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        String hrefList = tridentTuple.getString(0);
        Integer depth = Integer.parseInt(tridentTuple.getString(1));

        if(hrefList == null || hrefList.trim().length() == 0 || depth == 0)
            return true;//Always pass tuple downstream. However skip recursively adding this Href URL to kafka.
        depth--;

        String[] hrefArray = hrefList.split(" ");
        for(String href : hrefArray) {
            producer.send(new KeyedMessage<String, String>(kafkaTopic, href+" "+depth.toString()));
        }

        return true;
    }
}