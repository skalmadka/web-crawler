package storm.crawler;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

/**
 * Created by Sunil Kalmadka on 4/5/15.
 */
public final class CrawlerConfig {

    public static final String ELASTICSEARCH_CLUSTER_NAME = "elasticsearch.cluster.name";

    public static final String ELASTICSEARCH_HOST_NAME = "elasticsearch.host.name";

    public static final String ELASTICSEARCH_HOST_PORT = "elasticsearch.host.port";

    public static final String ELASTICSEARCH_INDEX_NAME = "elasticsearch.index.name";

    public static final String KAFKA_TOPIC_NAME = "kafka.topic.crawl.name";

    public static final String KAFKA_CONSUMER_HOST_NAME = "kafka.consumer.host.name";

    public static final String KAFKA_CONSUMER_HOST_PORT = "kafka.consumer.host.port";

    public static final String KAFKA_PRODUCER_HOST_NAME = "kafka.producer.host.name";

    public static final String KAFKA_PRODUCER_HOST_PORT = "kafka.producer.host.port";

    public static final String REDIS_HOST_NAME = "redis.host.name";

    public static final String REDIS_HOST_PORT = "redis.host.port";

    public static final String BLOOM_FILTER_NAME = "bloomfilter.name";

    public static final String BLOOM_FILTER_EXPECTED_ELEMENT_COUNT = "bloomfilter.expected.count";

    public static final String BLOOM_FILTER_DESIRED_FALSE_POSITIVE = "bloomfilter.falsePositive";

}
