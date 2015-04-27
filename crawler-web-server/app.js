var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');

var routes = require('./routes/index');

//Helper Objects
var users = require('./users');

//Kafka
var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(process.env.KAFKA_ZK_HOST),
    producer = new Producer(client);

var topic_name_crawler = process.env.KAFKA_TOPIC_CRAWL;
var crawl_depth =  process.env.CRAWL_DEFAULT_DEPTH;

producer.on('ready', function () {
  console.log('Kafka Producer Ready');
});

producer.on('error', function (err) {
  console.log('Error:' + err);
});

//App
var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// uncomment after placing your favicon in /public
//app.use(favicon(__dirname + '/public/favicon.ico'));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', routes);

//API Routes
app.use('/api',function(req, res, next){
  console.log('Authenticating API Request...');
  var userid = req.body.userid;

  // userid isn't present
  if (!userid) return next(error(400, 'api userid required'));

  // key is invalid
  if (!~users[userid]) return next(error(401, 'invalid user'));

  // all good, store req.key for route access
  next();
});

app.post('/api/event', function(req, res, next){
  console.log('Looking for POST data...');
  //Get event from post data
  //Event : {type: file/url, uri:fileName/url, summary:<text>}
  var event = new Object();
  event.userid = req.body.userid;
  event.type = req.body.type;
  event.uri = req.body.uri;
  event.summary = req.body.summary;
  event.timestamp = req.body.timestamp;

  var payload_crawler = [{topic:topic_name_crawler , messages:event.uri+' '+crawl_depth}];

  console.log('Sending to kafka...');
  //Send to Kafka
  if(producer) {
    producer.send(payload_crawler, function (err, data) {
      if (err) {
        res.send(500, err);
      } else {
        res.send(200, 'Message is queued.');
      }
    });
  } else {
    res.send(500, 'Producer is not initialized');
  }
});


//URL Feeder
app.post('/api/url_feeder', function(req, res, next){
  console.log('Looking for POST data...');
  var event = new Object();
  event.userid = req.body.userid;
  event.uri = req.body.uri;
  event.crawl_depth = req.body.crawl_depth;

  var payload_crawler = [{topic:topic_name_crawler , messages:event.uri+' '+event.crawl_depth}];

  console.log('Sending to kafka...');
  //Send to Kafka
  if(producer) {
    producer.send(payload_crawler, function (err, data) {
      if (err) {
        res.send(500, err);
      } else {
        res.send(200, 'Message is queued.');
      }
    });
  } else {
    res.send(500, 'Producer is not initialized');
  }
});

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
  app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
      message: err.message,
      error: err
    });
  });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
  res.status(err.status || 500);
  res.render('error', {
    message: err.message,
    error: {}
  });
});

module.exports = app;
