var _ = require("underscore")._,
    request = require("request"),
    qs = require("querystring");

exports.createClient = function(_options, _cb) {
  _options = _.extend({
      url: "",
      apiKey: ""
    }, _options || {});

  var requestProxy = function(options, cb) {
    options.method = options.method || "GET";
    request(options, function(error, res, body) {
      var result = null;
      if (body && typeof body === "string") {
        try {
          result = JSON.parse(body);
        } catch(ex) {
          result = body;
        }
      }
      cb(error, result);
    });
  };

  var resolveTag = function(tag, thing) {
    return tag || (_options.tags && _options.tags[thing.name] ? _options.tags[thing.name] : null) || "master";
  };

  var apiUrlBase = _options.url + '/_rest/api/';
  var buildClientApi = function(thing, apiKey) {
    var api = {
      apiKey: apiKey,
      get: function(options, cb) {
        var tag = resolveTag(options.tag, thing);
        requestProxy({ 
          uri: apiUrlBase + thing.name + "/" + tag + "/" + options.id + "?token=" + this.apiKey
        }, cb);
      },
      list: function(options, cb) {
        var tag = resolveTag(options.tag, thing);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "/" + "?token=" + this.apiKey
        }, cb);
      },
      find: function(options, cb) {
        var tag = resolveTag(options.tag, thing);
        requestProxy({
          token: this.apiKey,
          uri: apiUrlBase + thing.name + "/" + tag + "/" + "?" + qs.stringify(options)
        }, cb);
      },
      save: function(options, cb) {
        var tag = resolveTag(options.tag, thing);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "?token=" + this.apiKey,
          method: "PUT",
          json: options
        }, cb);
      },
      remove: function(options, cb) {
        var tag = resolveTag(options.tag, thing);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "?token=" + this.apiKey,
          method: "DELETE",
          json: options
        }, cb);
      }
    };
    return api;
  };

  var client = {
    apiKey: _options.apiKey
  };
  
  // Build client api based on what the server reports back.
  request({
    method: "GET",
    uri: _options.url + "/_rest/airborne/api/",
    form: { token: client.apiKey }
  }, function(error, res, body) {
    if (!error && res.statusCode == 200) {
      var things = JSON.parse(body); // array of strings.
      _.each(things, function(thing) {
        client[thing.name] = buildClientApi(thing, client.apiKey);
      });
      _cb && _cb(null, client);
    } else {
      _cb && _cb("Unable to retrieve available APIs.");
    }
  });

  /* 

  1.  Make HTTP call to airborne meta/proxy end point.
  2.  Add routes to client object with HTTP proxy stuff.
  3.  Return client in callback.

  */  

};