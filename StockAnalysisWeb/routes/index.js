var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');

var router = express.Router();
var jsonParser = bodyParser.json();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.sendFile('index.html', {root: path.join(__dirname, '../views/')});
});


/* POST stock symbol */
/* perhaps do something else later */
router.post('/', jsonParser, function(req, res) {
  var stocksymbol = req.body.stocksymbol;
  console.log('get post request from webpage of symbol %s', stocksymbol);
  res.sendStatus(200);
});

module.exports = router;
