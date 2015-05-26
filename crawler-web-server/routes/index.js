var express = require('express');
var router = express.Router();

/* GET search page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Search' });
});

/* GET url_feeder page. */
router.get('/url_feeder', function(req, res, next) {
  res.render('url_feeder', { title: 'URL Feeder' });
});

/* GET voice_search page. */
router.get('/voice_search', function(req, res, next) {
  res.render('voice_search', { title: 'Voice Search' });
});


/* GET task_summary page. */
/*
router.get('/task_summary', function(req, res, next) {
  res.render('task_summary', { title: 'Task Summary' });
});
*/

module.exports = router;
