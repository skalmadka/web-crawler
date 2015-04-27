FROM google/nodejs

MAINTAINER Sunil Kalmadka skalmadka@gmail.com

WORKDIR /crawler_app
ADD package.json /crawler_app/
RUN npm install
ADD . /crawler_app

#Following Environment Variables MUST be set at run time. (Use -e option with docker run)
ENV PORT 3000
ENV KAFKA_ZK_HOST "127.0.0.1:2181"
ENV KAFKA_TOPIC_CRAWL "crawl"
ENV CRAWL_DEFAULT_DEPTH 3

EXPOSE $PORT
CMD []
ENTRYPOINT ["/nodejs/bin/npm", "start"]