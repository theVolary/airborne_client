var _ = require("underscore")._,
    request = require("request"),
    qs = require("querystring"),
    validate = require('json-schema').validate;

exports.createClient = function(_options, _cb) {
  _options = _.extend({
      url: ""
    }, _options || {});
  var apiUrlBase = _options.url + '/_rest/api/';

  function requestProxy(options, cb) {
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
  }

  function resolveTag(options, thing) {
    var tag = options && options.tag;
    return tag || (_options.tags && _options.tags[thing.name]
                    ? _options.tags[thing.name]
                    : _options.tag) || "master";
  }

  function doSave(thing, options, tag, cb) {
    options.doc.type = thing.name;
    options = runModRequestHook(options);
    var headers = extractHeaders(options); // This has to be here instead of in requestProxy because otherwise headers may get sent in the json below.
    requestProxy({
      uri: apiUrlBase + thing.name + "/" + tag,
      method: "PUT",
      json: options,
      headers: headers
    }, cb);
  }

  function doValidate(thing, options, schema, tag, cb) {
    var validationResult = validate(options.doc, schema);
    if (validationResult.valid) {
      doSave(thing, options, tag, cb);
    } else {
      var thingId = thing.name;
      if (tag && tag !== 'master') {
        thingId += ':tag:' + tag;
      }
      cb('Validation failed for instace of ' + thingId + '. Error: ' + JSON.stringify(validationResult));
    }
  }

  function extractHeaders(options) {
    var headers = null;
    if (options.headers) {
      headers = options.headers;
      delete options.headers;
    }
    return headers;
  }

  function runGetRequestHook(options) {
    if (_options.getRequestHookFn) {
      var hookOutput = _options.getRequestHookFn.call(this, options);
      if (hookOutput) options = hookOutput;
    }
    return options;
  }

  function runModRequestHook(options) {
    if (_options.modRequestHookFn) {
      var hookOutput = _options.modRequestHookFn.call(this, options);
      if (hookOutput) options = hookOutput;
    }
    return options;
  }

  function buildClientApi(thing) {
    var api = {
      thing: thing.thing,
      designDoc: thing.designDoc,
      schema: thing.schema,

      get: function(options, cb) {
        if (!options.id) {
          return cb('cannot call get without an id; thing name: ' + thing.name);
        }
        options = runGetRequestHook(options);
        var tag = resolveTag(options, thing);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "/" + options.id,
          headers: extractHeaders(options)
        }, cb);
      },

      list: function(options, cb) {
        var tag = resolveTag(options, thing);
        options = runGetRequestHook(options);
        var headers = extractHeaders(options);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "/list/?q=" + qs.escape(JSON.stringify(options)),
          headers: headers
        }, cb);
      },

      exists: function(options, cb) {
        var tag = resolveTag(options, thing);
        options = runGetRequestHook(options);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "/exists/" + options.id,
          headers: extractHeaders(options)
        }, cb);
      },

      find: function(options, cb) {
        var tag = resolveTag(options, thing);
        options = runGetRequestHook(options);
        var headers = extractHeaders(options);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag + "/" + "?q=" + qs.escape(JSON.stringify(options)),
          headers: headers
        }, cb);
      },

      save: function(options, cb) {
        var tag = resolveTag(options, thing);

        //before we attempt to send the data to airborne, validate on our side first if a schema is available
        if (api.schema) {
          doValidate(thing, options, api.schema, tag, cb);
        } else {
          doSave(thing, options, tag, cb);
        }
      },

      remove: function(options, cb) {
        var tag = resolveTag(options, thing);
        options = runModRequestHook(options);
        var headers = extractHeaders(options);
        requestProxy({
          uri: apiUrlBase + thing.name + "/" + tag,
          method: "DELETE",
          json: options,
          headers: headers
        }, cb);
      }
    };
    return api;
  }

  var client = {};

  // Build client api based on what the server reports back.
  request({
    method: "GET",
    uri: _options.url + "/_rest/airborne/api/"
  }, function(error, res, body) {
    if (!error && res.statusCode == 200) {
      var things = JSON.parse(body); // array of strings.

      //NOTE: this is using case-sensitive comparison
      if (_options.onlyInclude) {
        things = _.select(things, function(thing) {
          return _options.onlyInclude.indexOf(thing.name) > -1;
        });
      }

      //NOTE: this is using case-sensitive comparison
      if (_options.exclude) {
        things = _.reject(things, function(thing) {
          return _options.exclude.indexOf(thing.name) > -1;
        });
      }

      var sem = things.length;

      _.each(things, function(thing) {
        client[thing.name] = buildClientApi(thing);
      });

      _cb(null, client);

    } else {
      _cb && _cb("Unable to retrieve available APIs from " + _options.url + ' due to: ' + require('util').inspect(error));
    }
  });

};