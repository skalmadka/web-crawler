package storm.crawler;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import com.github.fhuss.storm.elasticsearch.state.QuerySearchIndexQuery;
import com.sun.tools.internal.jxc.ConfigReader;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import storm.crawler.filter.KafkaProducerFilter;
import storm.crawler.filter.PrintFilter;
import storm.crawler.filter.URLFilter;
import storm.crawler.function.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import storm.crawler.state.ESTridentTupleMapper;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.*;
import java.lang.System;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class WebCrawlerTopology {
    public static StormTopology buildTopology(Config conf, LocalDRPC localDrpc) {
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
        TridentState esStaticState = topology.newStaticState(esStateFactory);

        //Topology
        topology.newStream("crawlKafkaSpout", spout).parallelismHint(5)
                 //Splits url and depth information on receiving from Kafka
                .each(new Fields("str"), new SplitKafkaInput(), new Fields("url", "depth"))
                //Bloom Filter. Filters already crawled URLs
                .each(new Fields("url"), new URLFilter())
                //Download and Parse Webpage
                .each(new Fields("url"), new GetAdFreeWebPage(), new Fields("content_html", "title", "href"))//TODO Optimize
                //Add Href URls to Kafka queue
                .each(new Fields("href", "depth"), new KafkaProducerFilter())//TODO Replace with kafka persistent state.
                //Insert to Elasticsearch
                .each(new Fields("url", "content_html", "title"), new PrepareForElasticSearch(), new Fields("index", "type", "id", "source"))
                .partitionPersist(esStateFactory, new Fields("index", "type", "id", "source"), new ESIndexUpdater<String>(new ESTridentTupleMapper()))
        ;

        //DRPC
        topology.newDRPCStream("search", localDrpc)
                .each(new Fields("args"), new SplitDRPCArgs(), new Fields("query_input"))
                .each(new Fields("query_input"), new BingAutoSuggest(0), new Fields("query_preProcessed"))//TODO return List of expanded query
                .each(new Fields("query_preProcessed"), new PrepareSearchQuery(), new Fields("query", "indices", "types"))
                .groupBy(new Fields("query", "indices", "types"))
                .stateQuery(esStaticState, new Fields("query", "indices", "types"), new QuerySearchIndexQuery(), new Fields("results"))
        ;

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        if(args.length < 1){
            System.err.println("[ERROR] Configuration File Required");
        }
        Config conf = new Config();

        // Store all the configuration in the Storm conf object
        conf.putAll(readConfigFile(args[0]));

        //Second arg should be local in order to run locally
        if(args.length  < 2 || (args.length  == 2 && !args[1].equals("local"))) {
            StormSubmitter.submitTopologyWithProgressBar("crawler_topology", conf, buildTopology(conf, null));
        }
        else {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster localcluster = new LocalCluster();
            localcluster.submitTopology("crawler_topology",conf,buildTopology(conf, drpc));

            String searchQuery = "elasticsearch";
            System.out.println("---* Result (search): " + drpc.execute("search",  searchQuery));
        }
    }

    private static Map readConfigFile(String filename) throws IOException {
        Map ret;
        Yaml yaml = new Yaml(new SafeConstructor());
        InputStream inputStream = new FileInputStream(new File(filename));

        try {
            ret = (Map)yaml.load(inputStream);
        } finally {
            inputStream.close();
        }

        if(ret == null) {
            ret = new HashMap();
        }

        return new HashMap(ret);
    }
}
