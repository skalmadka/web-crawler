
var config = {}

config.kafka_topic_crawl_name="crawl";
config.kafka_consumer_host_name="127.0.0.1";
config.kafka_consumer_host_port=2181;

config.web_server_port=9080
config.crawl_depth=3

module.exports = config;