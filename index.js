/**
 * Copyright (c) 2016-2017 Cirrus Link Solutions
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Cirrus Link Solutions
 */

var mqtt = require('mqtt'),
    kurapayload = require('./lib/kurapayload.js'),
    sparkplugbpayload = require('./lib/sparkplugbpayload.js'),
    events = require('events'),
    util = require("util"),
    pako = require('pako'),
    logger = require('log4jcore'),
    _ = require('lodash');

var compressed = "SPBV1.0_COMPRESSED";

var log = logger('SparkPlugClient');

var getRequiredProperty = function(config, propName) {
    if (config[propName] !== undefined) {
        return config[propName];
    }
    throw new Error("Missing required configuration property '" + propName + "'");
};

var getProperty = function(config, propName, defaultValue) {
    if (config[propName] !== undefined) {
        return config[propName];
    } else {
        return defaultValue;
    }
};

/*
 * Sparkplug Client
 */
function SparkplugClient(config) {
    var versionA = "spAv1.0",
        versionB = "spBv1.0",
        serverUrl = getRequiredProperty(config, "serverUrl"),
        username = getRequiredProperty(config, "username"),
        password = getRequiredProperty(config, "password"),
        clientId = getRequiredProperty(config, "clientId"),

        // true if this node is an application node (e.g. an Ignition-like application),
        // false if this node is a device or edge node
        isApplication = getProperty(config, "isApplication"),

        // groupId and edgeNode are required for device (non-application) nodes, and ignored
        // for application nodes
        groupId = isApplication ? getProperty(config, "groupId") : getRequiredProperty(config, "groupId"),
        edgeNode = isApplication ? getProperty(config, "edgeNode") : getRequiredProperty(config, "edgeNode"),
        appName = getProperty(config, "appName") || 'APP',
        appStateTopic = "STATE/" + appName,

        publishDeath = getProperty(config, "publishDeath", false),
        version = getProperty(config, "version", versionB),
        bdSeq = 0,
        seq = 0,
        devices = [],
        client = null,
        connected = false,

    // Increments a sequence number
    incrementSeqNum = function() {
        if (seq == 256) {
            seq = 0;
        }
        return seq++;
    },

    encodePayload = function(payload) {
        if (version === versionA) {
            return kurapayload.generateKuraPayload(payload);
        } else {
            return sparkplugbpayload.encodePayload(payload);
        }
    },

    decodePayload = function(payload) {
        if (version === versionA) {
            return kurapayload.parseKuraPayload(payload);
        } else {
            return sparkplugbpayload.decodePayload(payload);
        }
    },

    addSeqNumber = function(payload) {
        if (version === versionA) {
            payload.metric = payload.metric !== undefined
                ? payload.metric
                : [];
            payload.metric.push({ "name" : "seq", "value" : incrementSeqNum(), "type" : "int" });
        } else {
            payload.seq = incrementSeqNum();
        }   
    },

    // Get DEATH payload
    getDeathPayload = function() {
        var payload = {
                "timestamp" : new Date().getTime()
            },
            metric = [ {
                "name" : "bdSeq", 
                "value" : bdSeq, 
                "type" : "int"
            } ];
        if (version === versionA) {
            payload.metric = metric;
        } else {
            payload.metrics = metric;
        }
        return payload;
    },

    // Publishes DEATH certificates for the edge node
    publishNDeath = function(client) {
        var payload, topic;

        // Publish DEATH certificate for edge node
        log.debug("Publishing Edge Node Death");
        payload = getDeathPayload();
        topic = version + "/" + groupId + "/NDEATH/" + edgeNode;
        client.publish(topic, encodePayload(payload));
        messageAlert("published", topic, payload);
    },

    // Logs a message alert to the console
    messageAlert = function(alert, topic, payload) {
        log.debug("Message " + alert);
        log.debug(" topic: " + topic);
        log.debug(" payload: " + JSON.stringify(payload));
    },

    compressPayload = function(payload, options) {
        var algorithm = null,
            compressedPayload,
            resultPayload = {
                "uuid" : compressed
            };

        log.debug("Compressing payload " + JSON.stringify(options));

        // See if any options have been set
        if (options !== undefined && options !== null) {
            // Check algorithm
            if (options['algorithm']) {
                algorithm = options['algorithm'];
            }
        }

        if (algorithm === null || algorithm.toUpperCase() === "DEFLATE") {
            log.debug("Compressing with DEFLATE!");
            resultPayload.body = pako.deflate(payload);
        } else if (algorithm.toUpperCase() === "GZIP") {
            log.debug("Compressing with GZIP");
            resultPayload.body = pako.gzip(payload);
        } else {
            throw new Error("Unknown or unsupported algorithm " + algorithm);
        }

        // Create and add the algorithm metric if is has been specified in the options
        if (algorithm !== null) {
            resultPayload.metrics = [ {
                "name" : "algorithm", 
                "value" : algorithm.toUpperCase(), 
                "type" : "string"
            } ];
        }

        return resultPayload;
    },

    decompressPayload = function(payload) {
        var metrics = payload.metrics,
            algorithm = null;

        log.debug("Decompressing payload");

        if (metrics !== undefined && metrics !== null) {
            for (var i = 0; i < metrics.length; i++) {
                if (metrics[i].name === "algorithm") {
                    algorithm = metrics[i].value;
                }
            }
        }

        if (algorithm === null || algorithm.toUpperCase() === "DEFLATE") {
            log.debug("Decompressing with DEFLATE!");
            return pako.inflate(payload.body);
        } else if (algorithm.toUpperCase() === "GZIP") {
            log.debug("Decompressing with GZIP");
            return pako.ungzip(payload.body);
        } else {
            throw new Error("Unknown or unsupported algorithm " + algorithm);
        }

    },

    maybeCompressPayload = function(payload, options) {
        return options && options.compress ?
          compressPayload(encodePayload(payload), options): payload;
    },

    maybeDecompressPayload = function(payload) {
        return payload.uuid === compressed ?
          decodePayload(decompressPayload(payload)) : payload
    };

    events.EventEmitter.call(this);

    // Publishes Node BIRTH certificates for the edge node
    this.publishNodeBirth = function(payload, options) {
        var topic = version + "/" + groupId + "/NBIRTH/" + edgeNode;
        // Reset sequence number
        seq = 0;
        // Add seq number
        addSeqNumber(payload);
        // Add bdSeq number
        var metrics = payload.metrics
        if (metrics !== undefined && metrics !== null) {
            metrics.push({
                "name" : "bdSeq",
                "type" : "uint32", 
                "value" : bdSeq
            });
        }

        // Publish BIRTH certificate for edge node
        log.debug("Publishing Edge Node Birth");
        var p = maybeCompressPayload(payload, options);
        client.publish(topic, encodePayload(p));
        messageAlert("published", topic, p);
    };

    function publishWithSeq(topic, payload, options) {
      addSeqNumber(payload);
      client.publish(topic, encodePayload(maybeCompressPayload(payload, options)));
      messageAlert("published", topic, payload);
    }

    // Publishes Node Data messages for the edge node
    this.publishNodeData = function(payload, options) {
        log.debug("Publishing NDATA");
        publishWithSeq(version + "/" + groupId + "/NDATA/" + edgeNode,
          payload, options);
    };

    // Publishes Node Data messages for the edge node
    this.publishNodeCommand = function(request) {
        function requireField(fieldName, message) {
            if(!request[fieldName])
                throw new Error("request." + fieldName + " must be a non empty string");
          return request[fieldName];
        }
        var groupId = requireField('groupId');
        var nodeId = requireField('nodeId');
        var timestamp = request.timestamp || Date.now()
        var metrics = request.metrics
        if(!metrics || !Array.isArray(metrics)) {
            throw new Error("request.metrics must be an array of SparkPlug metrics with the structure [{name: string, value: number | string, type: string}]");
        }
        log.debug("Publishing NCMD");
        var topic = version + "/" + groupId + "/NCMD/" + nodeId;
        var payload = {timestamp: timestamp, metrics: metrics}
        client.publish(topic, encodePayload(maybeCompressPayload(payload, {})));
        messageAlert("published", topic, payload);
    };

    // Publishes Node BIRTH certificates for the edge node
    this.publishDeviceData = function(deviceId, payload, options) {
        log.debug("Publishing DDATA for device " + deviceId);
        publishWithSeq(version + "/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId,
            payload, options);
    };

    // Publishes Node BIRTH certificates for the edge node
    this.publishDeviceBirth = function(deviceId, payload, options) {
        log.debug("Publishing DBIRTH for device " + deviceId);
        publishWithSeq(version + "/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId,
            payload, options);
    };

    // Publishes Node BIRTH certificates for the edge node
    this.publishDeviceDeath = function(deviceId, payload) {
        log.debug("Publishing DDEATH for device " + deviceId);
        publishWithSeq(version + "/" + groupId + "/DDEATH/" + edgeNode + "/" + deviceId,
            payload, options);
    };

    this.stop = function() {
        log.debug("publishDeath: " + publishDeath);
        if (publishDeath) {
            // Publish the DEATH certificate
            publishNDeath(client);
        }
        client.end();
    };

    // Configures and connects the client
    return (function(sparkplugClient) {
        // Client connection options
        var willMessage = isApplication ? {
            topic: appStateTopic,
            payload: 'OFFLINE',
            qos: 1,
            retain: true
        } : {
          topic: version + "/" + groupId + "/NDEATH/" + edgeNode,
          payload: encodePayload(getDeathPayload()),
          qos: 0,
          retain: false
        };

        var clientOptions = {
            clientId: clientId,
            clean: true,
            keepalive: 30,
            connectionTimeout: 30,
            username: username,
            password: password,
            will: willMessage
        };

        // Connect to the MQTT server
        sparkplugClient.connecting = true;
        log.debug("Attempting to connect: " + serverUrl);
        log.debug("              options: " + JSON.stringify(clientOptions));
        client = mqtt.connect(serverUrl, clientOptions);
        log.debug("Finished attempting to connect");

        /*
         * 'connect' handler
         */
        client.on('connect', function () {
            log.debug("Client has connected");
            sparkplugClient.connecting = false;
            sparkplugClient.connected = true;
            sparkplugClient.emit("connect");

            if(isApplication) {
                client.publish(appStateTopic, 'ONLINE', {qos: 1, retain: true})
                var messagesFromDevices = ["NBIRTH", "NDEATH", "DBIRTH", "DDEATH", "NDATA", "DDATA"]
                messagesFromDevices.forEach(function(messageType) {
                    //         wildcards for: groupId              edgeNode
                    client.subscribe(version + "/+/" + messageType + "/+/#", { "qos" : 0 })
                })
            } else {
              // Subscribe to control/command messages for both the edge node and the attached devices
              log.debug("Subscribing to control/command messages for both the edge node and the attached devices");
              client.subscribe(version + "/" + groupId + "/NCMD/" + edgeNode + "/#", { "qos" : 0 });
              client.subscribe(version + "/" + groupId + "/DCMD/" + edgeNode + "/#", { "qos" : 0 });
              client.subscribe("STATE/+", {qos: 1});
              // Emit the "birth" event to notify the application to send a births
              sparkplugClient.emit("birth");
            }
        });

        /*
         * 'error' handler
         */
        client.on('error', function(error) {
            if (sparkplugClient.connecting) {
                sparkplugClient.emit("error", error);
                client.end();
            }
        });

        /*
         * 'close' handler
         */
        client.on('close', function() {
            if (sparkplugClient.connected) {
                sparkplugClient.connected = false;
                sparkplugClient.emit("close");
            }
        });

        /*
         * 'reconnect' handler
         */
        client.on("reconnect", function() {
            sparkplugClient.emit("reconnect");
        });

        /*
         * 'message' handler
         */
        client.on('message', function (topic, message) {
          // Split the topic up into tokens
          var splitTopic = topic.split("/");
          var splitTopicBegin = splitTopic.slice(0, 4)
          if ("STATE" === splitTopic[0]) {
            var host = splitTopic[1]
            if (host) {
                var status = message.toString()
                switch (status) {
                  case "ONLINE": sparkplugClient.emit("scadaHostOnline", host); break;
                  case "OFFLINE": sparkplugClient.emit("scadaHostOffline", host); break;
                  default: log.error("Got unrecognized STATE payload: " + status);
                }
            } else {
                log.error("Got STATE message with no host")
            }
          } else {
            var payload = maybeDecompressPayload(decodePayload(message));
            messageAlert("arrived", topic, payload);
            if(isApplication) {
              try {
                if(splitTopic.length < 4) throw new Error("unexpected topic: " + topic)
                if(version !== splitTopic[0]) throw new Error("unexpected version: " + splitTopic[0])
                sparkplugClient.emit("appMessage", {
                  messageType: splitTopic[2],
                  groupId: splitTopic[1],
                  edgeNode: splitTopic[3],
                  payload: payload
                })
              } catch (err) {
                log.error("Application got unexpected message: " + err.message)
              }
            } else { // Device node

              if (_.isEqual([version, groupId, "NCMD", edgeNode], splitTopicBegin)) {
                // Emit the "command" event
                sparkplugClient.emit("ncmd", payload);
              } else if (_.isEqual([version, groupId, "DCMD", edgeNode], splitTopicBegin)) {
                // Emit the "command" event for the given deviceId
                sparkplugClient.emit("dcmd", splitTopic[4], payload);
              } else {
                log.error("Message received on unknown topic " + topic);
              }
            }
          }
        });

        return sparkplugClient;
    }(this));
};

util.inherits(SparkplugClient, events.EventEmitter);

exports.newClient = function(config) {
    return new SparkplugClient(config);
};