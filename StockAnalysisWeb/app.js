var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var redis = require('redis');

var argv = require('minimist')(process.argv.slice(2));
var redis_host = argv['redis_host'];
var redis_port = argv['redis_port'];
var redis_channel = argv['redis_channel'];

var index = require('./routes/index');

var app = express();
var server = require('http').createServer(app);

var io = require('socket.io')(server);

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// uncomment after placing your favicon in /public
//app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

var redisClient = redis.createClient(redis_port, redis_host);
redisClient.subscribe(redis_channel);
console.log('subscribe to redis channel ' + String(redis_channel));

// when new message arrives, invoke function
redisClient.on('message', function (channel, message) {
  if (channel == redis_channel) {
    console.log('message received %s', message);
    io.sockets.emit('data', message)
  }
});

app.use('/', index);
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist'));
app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist'));
app.use('/d3', express.static(__dirname + '/node_modules/d3'));
app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build'));
app.use('/public', express.static(__dirname + '/public'));

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

server.listen(3000, function() {
  console.log('server started at port 3000')
});

// set up shutdown hooks
var shutdown_hook = function() {
  console.log('Quitting redis client');
  redisClient.quit();
  console.log('Shutting down app');
  process.exit()
};

process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);