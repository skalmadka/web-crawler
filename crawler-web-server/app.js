var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');

var routes = require('./routes/index');

//Helper Objects
var users = require('./users');

//Storm DRPC
var NodeDRPCClient = require('node-drpc');
var nodeDrpcClient =  new  NodeDRPCClient( process.env.DRPC_SERVER_HOST,  parseInt(process.env.DRPC_SERVER_PORT, 10) );

//Kafka
var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(process.env.KAFKA_ZK_HOST),
    producer = new Producer(client);

var topic_name_crawler = process.env.KAFKA_TOPIC_CRAWL;
var crawl_depth =  process.env.CRAWL_DEFAULT_DEPTH;
var topic_name_doc = process.env.KAFKA_TOPIC_DOC;

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

  // all good
  next();
});

app.post('/api/event', function(req, res, next){
  console.log('/api/event: Looking for POST data...');
  //Get event from post data
  var event = new Object();
  event.userid = req.body.userid;
  event.uri = req.body.uri;

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
    res.send(500, 'Unable to send to Kafka Producer');
  }
});

//DRPC Search Route
app.post('/api/search', function(req, res, next){
  console.log('/api/search: Looking for POST data...');

  var userid = req.body.userid;
  var searchQuery = req.body.searchQuery;

  nodeDrpcClient.execute("search", searchQuery, function(err, response) {//DRPC func_name, func_args, callback
    if (err) {
      console.error(err);
    } else {
      console.log("Storm DRPC Success");

      var searchResultArray = [];
      var jsonResponse = JSON.parse(response);
      for(var i = 0; i < jsonResponse.length; i++) {
        var curResponse = jsonResponse[i];
        var curSearchResult = curResponse[curResponse.length -1];
        searchResultArray.push(curSearchResult)

        console.log("curResponse"+ i + " : "+JSON.stringify(curResponse));
      }

      console.log("searchResultArray"+ JSON.stringify(searchResultArray));
      res.render('search_result', {searchResult: searchResultArray});
      /*res.render('search_result', {searchResult: searchResultArray}, function(err, html) {
       res.send(200, JSON.stringify(searchResultArray));
       })*/
    }
  });
});

//Voice Search
app.get('/speech_service', function(req, res, next){
  console.log('/speech_service: Looking for GET data...');

  var user_utterance = req.query.utterance;
  console.log('/speech_service:'+user_utterance);
  nodeDrpcClient.execute("search", user_utterance, function(err, response) {//DRPC func_name, func_args, callback
    if (err) {
      console.error(err);
    } else {
      console.log("Storm DRPC Success");

      var searchResultArray = [];
      var jsonResponse = JSON.parse(response);
      for(var i = 0; i < jsonResponse.length; i++) {
        var curResponse = jsonResponse[i];
        var curSearchResult = curResponse[curResponse.length -1];
        searchResultArray.push(curSearchResult)

        console.log("curResponse"+ i + " : "+JSON.stringify(curResponse));
      }

      var count =  (searchResultArray.length == 0)? "no" : searchResultArray.length;

      console.log("searchResultArray"+ JSON.stringify(searchResultArray));
      res.send(200, {current_response:count, search_results: searchResultArray});

      //res.render('search_result', {searchResult: searchResultArray});

    }
  });
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
