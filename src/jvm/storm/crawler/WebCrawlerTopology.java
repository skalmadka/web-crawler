package storm.crawler;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import storm.crawler.filter.KafkaProducerFilter;
import storm.crawler.filter.PrintFilter;
import storm.crawler.filter.URLFilter;
import storm.crawler.function.GetAdFreeWebPage;
import storm.crawler.function.PrepareCrawledPageDocument;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import storm.crawler.state.ESTridentTupleMapper;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.System;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class WebCrawlerTopology {
    public static StormTopology buildTopology(Config conf) {
        TridentTopology topology = new TridentTopology();

        //Kafka Spout
        BrokerHosts zk = new ZkHosts(conf.get(CrawlerConfig.KAFKA_CONSUMER_HOST_NAME) + ":" +conf.get(CrawlerConfig.KAFKA_CONSUMER_HOST_PORT));
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zk, (String) conf.get(CrawlerConfig.KAFKA_TOPIC_NAME));
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);

        //ElasticSearch Persistent State
        Settings esSettings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", conf.get(CrawlerConfig.ELASTICSEARCH_CLUSTER_NAME))
                .put("storm.elasticsearch.hosts", conf.get(CrawlerConfig.ELASTICSEARCH_HOST_NAME) + ":" + conf.get(CrawlerConfig.ELASTICSEARCH_HOST_PORT))
                .build();
        StateFactory esStateFactory = new ESIndexState.Factory<String>(new ClientFactory.NodeClient(esSettings.getAsMap()), String.class);

/*        //Kafka State
        TridentKafkaStateFactory kafkaStateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector("crawl"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("refURL", "refDepth"));

        TridentState b1 =  s.each(new Fields("href", "depth"), new PrepareHrefKafka(), new Fields("refURL", "refDepth"))
        .partitionPersist(kafkaStateFactory, new Fields("refURL", "refDepth"), new TridentKafkaUpdaterEmitTuple(), new Fields());
*/
        //Topology
        topology.newStream("crawlKafkaSpout", spout).parallelismHint(5)
                .each(new Fields("str"), new PrintFilter())
                //Bloom Filter
                .each(new Fields("str"), new URLFilter())
                //Download and Parse Webpage
                .each(new Fields("str"), new GetAdFreeWebPage(), new Fields("url", "content_html", "title", "href", "depth"))
                //Kafka Send: Recursive Href
                .each(new Fields("href", "depth"), new KafkaProducerFilter())
                //Insert to Elasticsearch
                .each(new Fields("url", "content_html", "title", "href"), new PrepareCrawledPageDocument(), new Fields("index", "type", "id", "source"))
                .partitionPersist(esStateFactory, new Fields("index", "type", "id", "source"), new ESIndexUpdater<String>(new ESTridentTupleMapper()), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 1){
            System.err.println("[ERROR] Properties File Required");
        }
        Config conf = new Config();

        Properties prop = new Properties();
        try{
            Reader propReader = new FileReader(new File(args[0]));
            prop.load(propReader);
        } catch(IOException e){
            System.err.println("[ERROR] Unable to read Properties File "+args[0]);
            e.printStackTrace();
            return;
        }

        for(Map.Entry property : prop.entrySet()){
            conf.put((String)property.getKey(), property.getValue());
        }

        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("web_crawler", conf, buildTopology(conf));
        StormSubmitter.submitTopologyWithProgressBar("web_crawler", conf, buildTopology(conf));
    }
}
