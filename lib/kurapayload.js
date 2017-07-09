/**
 * Copyright (c) 2012, 2016 Cirrus Link Solutions
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Cirrus Link Solutions
 */

/**
 * Provides support for generating Kura payloads.
 */
(function () {
    var ProtoBuf = require("protobufjs");

    var Root = ProtoBuf.parse("package kuradatatypes; message KuraPayload { message KuraMetric { enum " +
        		"ValueType { DOUBLE = 0; FLOAT = 1; INT64 = 2; INT32 = 3; BOOL = 4; STRING = 5; BYTES = 6; } " +
        		"required string name = 1; required ValueType type = 2; optional double double_value = 3; optional " +
        		"float float_value = 4; optional int64 long_value = 5; optional int32 int_value = 6; optional bool" +
        		" bool_value = 7; optional string string_value = 8; optional bytes bytes_value = 9; } message " +
        		"KuraPosition { required double latitude = 1; required double longitude = 2; optional double altitude" +
        		" = 3; optional double precision = 4; optional double heading = 5; optional double speed = 6; " +
        		"optional int64 timestamp = 7; optional int32 satellites = 8; optional int32 status = 9; } " +
        		"optional int64 timestamp = 1; optional KuraPosition position = 2; extensions 3 to 4999; repeated " +
        		"KuraMetric metric = 5000; optional bytes body = 5001; }").root,
        KuraPayload = Root.lookup('kuradatatypes.KuraPayload'),
        KuraMetric = Root.lookup('kuradatatypes.KuraPayload.KuraMetric'),
        KuraPosition = Root.lookup('kuradatatypes.KuraPayload.KuraPosition'),
        ValueType = KuraMetric.ValueType;

    exports.generateKuraPayload = function(object) {
        var newPayload = KuraPayload.create({ 
                "timestamp" : object.timestamp,
                "metric" : []
            });

        // Build up the position
        if (object.position !== undefined && object.position !== null) {
            var position = object.position,
                newPosition = KuraPosition.create({
                    "latitude" : position.latitude, 
                    "longitude" : position.longitude
                });

            if (position.altitude !== undefined && position.altitude !== null) {
                newPosition.altitude = position.altitude;
            }
            if (position.precision !== undefined && position.precision !== null) {
                newPosition.precision = position.precision;
            }
            if (position.heading !== undefined && position.heading !== null) {
                newPosition.heading = position.heading;
            }
            if (position.speed !== undefined && position.speed !== null) {
                newPosition.speed = position.speed;
            }
            if (position.timestamp !== undefined && position.timestamp !== null) {
                newPosition.timestamp = position.timestamp;
            }
            if (position.satellites !== undefined && position.satellites !== null) {
                newPosition.satellites = position.satellites;
            }
            if (position.status !== undefined && position.status !== null) {
                newPosition.status = position.status;
            }
            // Add to KuraPayload
            newPayload.position = newPosition;
        }

        // Build up the metric
        if (object.metric !== undefined && object.metric !== null) {
            // loop over array of metric
            for (var i = 0; i < object.metric.length; i++) {
                var metric = object.metric[i],
                    newMetric = KuraMetric.create({ 
                        "name" : metric.name
                    }),
                    value = metric.value,
                    type = metric.type;
                // Get metric type and value
                switch (type) {
                case 'int':
                    newMetric.type = ValueType.INT32;
                    newMetric.intValue = value;
                    break;
                case 'long':
                    newMetric.type = ValueType.INT64;
                    newMetric.longValue = value;
                    break;
                case 'float':
                    newMetric.type = ValueType.FLOAT;
                    newMetric.floatValue = value;
                    break;
                case 'double':
                    newMetric.type = ValueType.DOUBLE;
                    newMetric.doubleValue = value;
                    break;
                case 'boolean':
                    newMetric.type = ValueType.BOOL;
                    newMetric.boolValue = value;
                    break;
                case 'string':
                    newMetric.type = ValueType.STRING;
                    newMetric.stringValue = value;
                    break;
                case 'bytes':
                    if (value instanceof ByteBuffer) {
                        newMetric.type = ValueType.BYTES;
                        newMetric.bytesValue = value;
                    } else {
                        throw new Error("Invalid object type for KuraMetric value");
                    }

                }
                newPayload.metric.push(newMetric);
            }
        }

        // set body
        if (object.body !== undefined && object.body !== null) {
            if (object.body instanceof ByteBuffer) {
                newPayload.bytes = object.body
            } else {
                throw new Error("Invalid object type for KuraPayload body");
            }
        }

        // Return the new KuraPayload as a Buffer
        return KuraPayload.encode(newPayload).finish();
    }

    exports.parseKuraPayload = function(proto) {
        var kuraPayload = KuraPayload.decode(proto),
            timestamp = kuraPayload.timestamp,
            kuraPosition = kuraPayload.position,
            kuraMetrics = kuraPayload.metric,
            body = kuraPayload.body,
            object = {};

        object.timestamp = timestamp.toNumber();

        if (position !== undefined) {
            var position = {};
            position.latitude = kuraPosition.latitude;
            position.longitude = kuraPosition.longitude;
            if (kuraPosition.altitude !== undefined) {
                position.altitude = kuraPostition.altitude;
            }
            if (kuraPosition.precision !== undefined) {
                position.precision = kuraPostition.precision;
            }
            if (kuraPosition.heading !== undefined) {
                position.heading = kuraPostition.heading;
            }
            if (kuraPosition.speed !== undefined) {
                position.speed = kuraPostition.speed;
            }
            if (kuraPosition.timestamp !== undefined) {
                position.timestamp = kuraPostition.timestamp;
            }
            if (kuraPosition.satellites !== undefined) {
                position.satellites = kuraPostition.satellites;
            }
            if (kuraPosition.status !== undefined) {
                position.status = kuraPostition.status;
            }
            object.position = position;
        }

        if (body !== undefined && body !== null) {
            object.body = body;
        }

        if (kuraMetrics != undefined) {
            metrics = [];
            // loop over array of metric
            for (var i = 0; i < kuraMetrics.length; i++) {
                var kuraMetric = kuraMetrics[i],
                metric = {};
                metric.name = kuraMetric.name;
                switch (kuraMetric.type) {
                case 0:
                    metric.value = kuraMetric.doubleValue;
                    metric.type = "double";
                    break;
                case 1:
                    metric.value = kuraMetric.floatValue;
                    metric.type = "float";
                    break;
                case 2:
                    metric.value = kuraMetric.longValue.toNumber();
                    metric.type = "long";
                    break;
                case 3:
                    metric.value = kuraMetric.intValue;
                    metric.type = "int";
                    break;
                case 4:
                    metric.value = kuraMetric.boolValue;
                    metric.type = "boolean";
                    break;
                case 5:
                    metric.value = kuraMetric.stringValue;
                    metric.type = "string";
                    break;
                case 6:
                    metric.value = kuraMetric.bytesValue;
                    metric.type = "bytes";
                }
                metrics.push(metric);
            }
            object.metric = metrics;
        }

        return object;
    }

}());
