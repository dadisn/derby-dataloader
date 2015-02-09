var derby = require('derby');
var redis = require('redis');
var livedb = require('livedb');
var liveDbMongo = require('livedb-mongo');
var parseUrl = require('url').parse;
var fs = require('fs');
var _ = require('lodash');
var defaults = require('./config/defaults');
var args = process.argv.slice(2);
var redisClient;
var redisObserver;

// Terminate if we don't have sufficient arguments
if(args.length < 2) {
  console.log('Usage: node index.js [collection] [data/filename]');
  console.log('The data provided should be JSON and an array of objects. Remember to wrap JSON with quotes.');
  console.log('If a filename is provided, the file should contain a valid JSON array.');
  process.exit();
}

// Configure process environmental variables
for(var key in defaults) {
  process.env[key] = process.env[key] || defaults[key];
}

// Get Redis configuration
if (process.env.REDIS_HOST) {
  redisClient = redis.createClient(process.env.REDIS_PORT, process.env.REDIS_HOST);
  redisObserver = redis.createClient(process.env.REDIS_PORT, process.env.REDIS_HOST);
  redisClient.auth(process.env.REDIS_PASSWORD);
  redisObserver.auth(process.env.REDIS_PASSWORD);
} else if(process.env.OPENREDIS_URL) {
  var redisUrl = parseUrl(process.env.OPENREDIS_URL);
  redisClient = redis.createClient(redisUrl.port, redisUrl.hostname);
  redisObserver = redis.createClient(redisUrl.port, redisUrl.hostname);
  redisClient.auth(redisUrl.auth.split(":")[1]);
  redisObserver.auth(redisUrl.auth.split(":")[1]);
} else {
  redisClient = redis.createClient();
  redisObserver = redis.createClient();
}

redisClient.select(process.env.REDIS_DB || 1);
redisObserver.select(process.env.REDIS_DB || 1);

// Set up the store that creates the model and syncs data
var db = liveDbMongo(process.env.MONGO_URL + '?auto_reconnect', {safe: true});
var driver = livedb.redisDriver(db, redisClient, redisObserver);
var backend = livedb.client({snapshotDb: db, driver: driver});
var store = derby.createStore({backend: backend});
var model = store.createModel({fetchOnly: true}, {});
var collection = args[0];

try {
  var obj = JSON.parse(args[1]);
  var arr = _.isArray(obj) ? obj : [obj];
} catch(e) {
  var path = './' + args[1];
  if(!fs.existsSync(path)) path = args[1];
  var arr = require(path);
}

// Some safeguards
if(!collection || typeof collection !== 'string') throw new Error('Please provide a valid collection name');
if(!_.isArray(arr)) throw new Error('Please provide an array of objects to add to the collection: ' + collection);

var len = arr.length;
var done = 0;

function terminate() {
  if(++done === len) {
    console.log(len + ' objects added to the collection ' + collection);
    process.exit(0);    
  }
}

model.fetch(collection, function(err) {
  if(err) throw err;

  for(var i = 0; i < len; i++) {
    model.add(collection, arr[i], terminate);
  }
});