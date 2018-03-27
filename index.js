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
    logger = require('winston'),
    _ = require('lodash');

var compressed = "SPBV1.0_COMPRESSED";

logger.level = 'debug';

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
    if(config.logLevel)
        logger.level = config.logLevel;
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
        logger.info("Publishing Edge Node Death");
        payload = getDeathPayload();
        topic = version + "/" + groupId + "/NDEATH/" + edgeNode;
        client.publish(topic, encodePayload(payload));
        messageAlert("published", topic, payload);
    },

    // Logs a message alert to the console
    messageAlert = function(alert, topic, payload) {
        logger.debug("Message " + alert);
        logger.debug(" topic: " + topic);
        logger.debug(" payload: " + JSON.stringify(payload));
    },

    compressPayload = function(payload, options) {
        var algorithm = null,
            compressedPayload,
            resultPayload = {
                "uuid" : compressed
            };

        logger.debug("Compressing payload " + JSON.stringify(options));

        // See if any options have been set
        if (options !== undefined && options !== null) {
                logger.info("test: " + options.algorithm);
            // Check algorithm
            if (options['algorithm']) {
                logger.info("test");
                algorithm = options['algorithm'];
            }
        }

        if (algorithm === null || algorithm.toUpperCase() === "DEFLATE") {
            logger.debug("Compressing with DEFLATE!");
            resultPayload.body = pako.deflate(payload);
        } else if (algorithm.toUpperCase() === "GZIP") {
            logger.debug("Compressing with GZIP");
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

        logger.debug("Decompressing payload");

        if (metrics !== undefined && metrics !== null) {
            for (var i = 0; i < metrics.length; i++) {
                if (metrics[i].name === "algorithm") {
                    algorithm = metrics[i].value;
                }
            }
        }

        if (algorithm === null || algorithm.toUpperCase() === "DEFLATE") {
            logger.debug("Decompressing with DEFLATE!");
            return pako.inflate(payload.body);
        } else if (algorithm.toUpperCase() === "GZIP") {
            logger.debug("Decompressing with GZIP");
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
        logger.info("Publishing Edge Node Birth");
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
        logger.info("Publishing NDATA");
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
        logger.info("Publishing NCMD");
        var topic = version + "/" + groupId + "/NCMD/" + nodeId;
        var payload = {timestamp: timestamp, metrics: metrics}
        client.publish(topic, encodePayload(maybeCompressPayload(payload, {})));
        messageAlert("published", topic, payload);
    };

    // Publishes Node BIRTH certificates for the edge node
    this.publishDeviceData = function(deviceId, payload, options) {
        logger.info("Publishing DDATA for device " + deviceId);
        publishWithSeq(version + "/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId,
            payload, options);
    };

    // Publishes Node BIRTH certificates for the edge node
    this.publishDeviceBirth = function(deviceId, payload, options) {
        logger.info("Publishing DBIRTH for device " + deviceId);
        publishWithSeq(version + "/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId,
            payload, options);
    };

    // Publishes Node BIRTH certificates for the edge node
    this.publishDeviceDeath = function(deviceId, payload) {
        logger.info("Publishing DDEATH for device " + deviceId);
        publishWithSeq(version + "/" + groupId + "/DDEATH/" + edgeNode + "/" + deviceId,
            payload, options);
    };

    this.stop = function() {
        logger.debug("publishDeath: " + publishDeath);
        if (publishDeath) {
            // Publish the DEATH certificate
            publishNDeath(client);
        }
        client.end();
    };

    // Configures and connects the client
    return (function(sparkplugClient) {
        // Client connection options
        var clientOptions = {
            "clientId" : clientId,
            "clean" : true,
            "keepalive" : 30,
            "connectionTimeout" : 30,
            "username" : username,
            "password" : password
        };
        if(!isApplication) {
            clientOptions.will = {
              "topic" : version + "/" + groupId + "/NDEATH/" + edgeNode,
              "payload" : encodePayload(getDeathPayload()),
              "qos" : 0,
              "retain" : false
            }
        }

        // Connect to the MQTT server
        sparkplugClient.connecting = true;
        logger.debug("Attempting to connect: " + serverUrl);
        logger.debug("              options: " + JSON.stringify(clientOptions));
        client = mqtt.connect(serverUrl, clientOptions);
        logger.debug("Finished attempting to connect");

        /*
         * 'connect' handler
         */
        client.on('connect', function () {
            logger.info("Client has connected");
            sparkplugClient.connecting = false;
            sparkplugClient.connected = true;
            sparkplugClient.emit("connect");

            if(isApplication) {
                var messagesFromDevices = ["NBIRTH", "NDEATH", "DBIRTH", "DDEATH", "NDATA", "DDATA"]
                messagesFromDevices.forEach(function(messageType) {
                    //         wildcards for: groupId              edgeNode
                    client.subscribe(version + "/+/" + messageType + "/+/#", { "qos" : 0 })
                })
            } else {
              // Subscribe to control/command messages for both the edge node and the attached devices
              logger.info("Subscribing to control/command messages for both the edge node and the attached devices");
              client.subscribe(version + "/" + groupId + "/NCMD/" + edgeNode + "/#", { "qos" : 0 });
              client.subscribe(version + "/" + groupId + "/DCMD/" + edgeNode + "/#", { "qos" : 0 });
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
            var payload = maybeDecompressPayload(decodePayload(message));
            messageAlert("arrived", topic, payload);

            // Split the topic up into tokens
            var splitTopic = topic.split("/");
            var splitTopicBegin = splitTopic.slice(0, 4)
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
                    logger.error("Application got unexpected message: " + err.message)
                }
            } else { // Device node
              if (_.isEqual([version, groupId, "NCMD", edgeNode], splitTopicBegin)) {
                // Emit the "command" event
                sparkplugClient.emit("ncmd", payload);
              } else if (_.isEqual([version, groupId, "DCMD", edgeNode], splitTopicBegin)) {
                // Emit the "command" event for the given deviceId
                sparkplugClient.emit("dcmd", splitTopic[4], payload);
              } else {
                logger.info("Message received on unknown topic " + topic);
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