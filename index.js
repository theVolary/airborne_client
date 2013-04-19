var _ = require("underscore")._,
    request = require("request"),
    qs = require("querystring"),
    validate = require('json-schema').validate;


exports.createClient = function(_options, _cb) {
  _options = _.extend({
      url: "",
      apiKey: ""
    }, _options || {});

  var requestProxy = function(options, cb) {
    options.method = options.method || "GET";
    request(options, function(error, res, body) {
      if (error) cb(error); else {
        var result = null;
        // inspect statusCode for non-200 block code (which means error)
        if (!res.statusCode || (res.statusCode < 200 || res.statusCode > 299)) {
          cb({
            statusCode: res.statusCode, 
            body: body || 'error making request'
          });
        } else {
          result = body;
          if (body && typeof body === "string") {
            try {
              result = JSON.parse(body);
            } catch(ex) {
              result = body;
            }
          }
          cb(null, result);
        }        
      }      
    });
  };

  var resolveTag = function(options, thing) {
    var tag = options && options.tag;
    return tag || (_options.tags && _options.tags[thing.name] 
                    ? _options.tags[thing.name] 
                    : _options.tag) || "master";
  };

  var apiUrlBase = _options.url + '/_rest/api/';
  var buildClientApi = function(thing, apiKey) {
    var api = {
      apiKey: apiKey,
      get: function(options, cb) {
        if (!options.id) {
          return cb('cannot call get without an id; thing name: ' + thing.name);
        }

        var tag = resolveTag(options, thing);
        requestProxy({ 
          uri: apiUrlBase + thing.name + "/" + tag + "/" + options.id + "?token=" + this.apiKey
        }, cb);
      },
      list: function(options, cb) {
        var tag = resolveTag(options, thing);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "/list/?q=" + qs.escape(JSON.stringify(options))
        }, cb);
      },
      exists: function(options, cb) {
        var tag = resolveTag(options, thing);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "/exists/" + options.id + "?token=" + this.apiKey
        }, cb);
      },
      find: function(options, cb) {
        var tag = resolveTag(options, thing);
        requestProxy({
          token: this.apiKey,
          uri: apiUrlBase + thing.name + "/" + tag + "/" + "?q=" + qs.escape(JSON.stringify(options))
        }, cb);
      },
      save: function(options, cb) {
        var tag = resolveTag(options, thing);

        //before we attempt to send the data to airborne, validate on our side first
        var thingId = thing.name;
        if (tag && tag !== 'master') {
          thingId += ':tag:' + tag;
        }

        var doSave = function() {
          options.doc.type = thing.name;
          requestProxy({
            uri: apiUrlBase + thing.name + "/" + tag + "?token=" + this.apiKey,
            method: "PUT",
            json: options
          }, cb);
        };

        var doValidate = function(schemaDoc) {
          var validationResult = validate(options.doc, schemaDoc.schema);
          if (validationResult.valid) {
            doSave();
          } else {
            cb('Validation failed for instace of ' + thingId + '. Error: ' + JSON.stringify(validationResult));
          }
        };

        if (api.schema) {
          doValidate(api.schema);
        } else {
          requestProxy({
            uri: _options.url + '/_rest/airborne/json-schema/' + thingId,
            method: 'GET'
          }, function(err, schemaDoc) {
            if (err) {
              cb("Unable to fetch validation document for '" + thingId + "'");
            } else {
              api.schema = schemaDoc;
              doValidate(schemaDoc);
            }
          });
        }
      },
      remove: function(options, cb) {
        var tag = resolveTag(options, thing);
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

      var sem = things.length;

      _.each(things, function(thing) {
        client[thing.name] = buildClientApi(thing, client.apiKey);

        //fetch the json-schemas in advance
        requestProxy({
          uri: _options.url + '/_rest/airborne/json-schema/' + thing.name,
          method: 'GET'
        }, function(err, schemaDoc) {
          if (err) {
            cb("Unable to fetch validation document for '" + thingId + "'");
          } else {
            client[thing.name].schema = schemaDoc.schema;

            sem--;
            if (sem == 0) {
              _cb && _cb(null, client);
            }
          }
        });
      });
      
    } else {
      _cb && _cb("Unable to retrieve available APIs.");
    }
  });

};