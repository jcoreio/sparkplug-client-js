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
    var _ = require("lodash");

    var SparkplugPayload = ProtoBuf.parse("package com.cirruslink.sparkplug.protobuf; message Payload { message Template { " +
            "message Parameter { optional string name = 1;optional uint32 type = 2; oneof value { uint32 int_value = 3; uint64 long_value = 4; " +
            "float float_value = 5; double double_value = 6; bool boolean_value = 7; string string_value = 8; ParameterValueExtension extension_value = 9; } " +
            "message ParameterValueExtension { extensions 1 to max; } } optional string version = 1; repeated Metric metrics = 2; " +
            "repeated Parameter parameters = 3; optional string template_ref = 4; optional bool is_definition = 5; extensions 6 to max; } " +
            "message DataSet { " +
            "message DataSetValue { oneof value { uint32 int_value = 1; uint64 long_value = 2; float float_value = 3; double double_value = 4; " +
            "bool boolean_value = 5; string string_value = 6; DataSetValueExtension extension_value = 7; } " +
            "message DataSetValueExtension { extensions 1 to max; } } " +
            "message Row { repeated DataSetValue elements = 1; extensions 2 to max; } optional uint64 num_of_columns = 1; repeated string columns = 2; " +
            "repeated uint32 types = 3; repeated Row rows = 4; extensions 5 to max; } " +
            "message PropertyValue { optional uint32 type = 1; optional bool is_null = 2;  oneof value { uint32 int_value = 3; uint64 long_value = 4; " +
            "float float_value = 5; double double_value = 6; bool boolean_value = 7; string string_value = 8; PropertySet propertyset_value = 9; " +
            "PropertySetList propertysets_value = 10; PropertyValueExtension extension_value = 11; } " +
            "message PropertyValueExtension { extensions 1 to max; } } " +
            "message PropertySet { repeated string keys = 1; repeated PropertyValue values = 2; extensions 3 to max; } " +
            "message PropertySetList { repeated PropertySet propertyset = 1; extensions 2 to max; } " +
            "message MetaData { optional bool is_multi_part = 1; optional string content_type = 2; optional uint64 size = 3; optional uint64 seq = 4; " +
            "optional string file_name = 5; optional string file_type = 6; optional string md5 = 7; optional string description = 8; extensions 9 to max; } " +
            "message Metric { optional string name = 1; optional uint64 alias = 2; optional uint64 timestamp = 3; optional uint32 datatype = 4; " +
            "optional bool is_historical = 5; optional bool is_transient = 6; optional bool is_null = 7; optional MetaData metadata = 8; " +
            "optional PropertySet properties = 9; oneof value { uint32 int_value = 10; uint64 long_value = 11; float float_value = 12; double double_value = 13; " +
            "bool boolean_value = 14; string string_value = 15; bytes bytes_value = 16; DataSet dataset_value = 17; Template template_value = 18; " +
            "MetricValueExtension extension_value = 19; } " +
            "message MetricValueExtension { extensions 1 to max; } } optional uint64 timestamp = 1; repeated Metric metrics = 2; optional uint64 seq = 3; " +
            "optional string uuid = 4; optional bytes body = 5; extensions 6 to max; } ").root,
        Payload = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload'),
        Template = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.Template'),
        Parameter = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.Template.Parameter'),
        DataSet = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.DataSet'),
        DataSetValue = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.DataSet.DataSetValue'),
        Row = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.DataSet.Row'),
        PropertyValue = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.PropertyValue'),
        PropertySet = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.PropertySet'),
        PropertyList = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.PropertyList'),
        MetaData =SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.MetaData'),
        Metric = SparkplugPayload.lookup('com.cirruslink.sparkplug.protobuf.Payload.Metric');

    /**
     * Sets the value of an object given it's type expressed as an integer
     */
    setValue = function(type, value, object) {
        switch (type) {
            case 1: // Int8
            case 2: // Int16
            case 3: // Int32
            case 5: // UInt8
            case 6: // UInt32
                object.intValue = value;
                break;
            case 4: // Int64
            case 7: // UInt32
            case 8: // UInt64
            case 13: // DataTime
                object.longValue = value;
                break;
            case 9: // Float
                object.floatValue = value;
                break;
            case 10: // Double
                object.doubleValue = value;
                break;
            case 11: // Boolean
                object.booleanValue = value;
                break;
            case 12: // String
            case 14: // Text
            case 15: // UUID
                object.stringValue = value;
                break;
            case 16: // DataSet
                object.datasetValue = encodeDataSet(value);
                break;
            case 17: // Bytes
            case 18: // File
                object.bytesValue = value;
                break;
            case 19: // Template
                object.templateValue = encodeTemplate(value);
                break;
            case 20: // PropertySet
                object.propertysetValue = encodePropertySet(value);
                break;
            case 21:
                object.propertysetsValue = encodePropertySetList(value);
                break;
        } 
    }

    getValue = function(type, object) {
        switch (type) {
            case 1: // Int8
            case 2: // Int16
            case 3: // Int32
            case 5: // UInt8
            case 6: // UInt32
                return object.intValue;
            case 4: // Int64
            case 7: // UInt32
            case 8: // UInt64
            case 13: // DataTime
                return object.longValue;
            case 9: // Float
                return object.floatValue;
            case 10: // Double
                return object.doubleValue;
            case 11: // Boolean
                return object.booleanValue;
            case 12: // String
            case 14: // Text
            case 15: // UUID
                return object.stringValue;
            case 16: // DataSet
                return decodeDataSet(object.datasetValue);
            case 17: // Bytes
            case 18: // File
                return object.bytesValue;
            case 19: // Template
                return decodeTemplate(object.templateValue);
            case 20: // PropertySet
                return decodePropertySet(object.propertysetValue);
            case 21:
                return decodePropertySetList(object.propertysetsValue);
            default:
                return null;
        } 
    }

    var TypesDict = {
        Int8: 1,
        Int16: 2,
        Int32: 3,
        Int64: 4,
        UInt8: 5,
        UInt16: 6,
        UInt32: 7,
        UInt64: 8,
        Float: 9,
        Double: 10,
        Boolean: 11,
        String: 12,
        Text: 14,
        UUID: 15,
        DataSet: 16,
        Bytes: 17,
        File: 18,
        Template: 19,
        PropertySet: 20,
        PropertySetList: 21
    }

    // Create a version of TypesDict where the type names are all uppercase
    var TypesDictUppercase = _.mapKeys(TypesDict, function(value, key) { return key.toUpperCase(); });

    var encodeType = function(typeString) {
      return TypesDictUppercase[typeString.toUpperCase()] || 0;
    }

    // Create a version of TypesDict that maps the integer type ID to the string type
    var TypesDictReversed = _.invert(TypesDict);

    var decodeType = function(typeInt) {
        return TypesDictReversed[typeInt];
    }

    // Turn a function that operates on a single instance into a function that transforms
    // an array of instances
    var arrayTransform = function(transformFunc) {
        return function(array) {
          return array.map(transformFunc)
        }
      }

    var encodeTypes = arrayTransform(encodeType);
    var decodeTypes = arrayTransform(decodeType);

    encodeDataSet = function(object) {
        var num = object.numOfColumns,
            names = object.columns,
            types = encodeTypes(object.types),
            rows = object.rows,
            newDataSet = DataSet.create({
                "numOfColumns" : num, 
                "columns" : object.columns, 
                "types" : types 
            }),
            newRows = [];
        // Loop over all the rows
        for (var i = 0; i < rows.length; i++) {
            var newRow = Row.create();
                row = rows[i];
                elements = [];
            // Loop over all the elements in each row
            for (var t = 0; t < num; t++) {
                var newValue = DataSetValue.create();
                setValue(types[t], row[t], newValue);
                elements.push(newValue);
            }
            newRow.elements = elements;
            newRows.push(newRow);
        }
        newDataSet.rows = newRows;
        return newDataSet;
    }

    decodeDataSet = function(protoDataSet) {
        var dataSet = {},
            protoTypes = protoDataSet.types,
            types = decodeTypes(protoTypes),
            protoRows = protoDataSet.rows,
            num = protoDataSet.numOfColumns,
            rows = [];
        
        // Loop over all the rows
        for (var i = 0; i < protoRows.length; i++) {
            var protoRow = protoRows[i],
                protoElements = protoRow.elements,
                row = [];
            // Loop over all the elements in each row
            for (var t = 0; t < num; t++) {
                row.push(getValue(protoTypes[t], protoElements[t]));
            }
            rows.push(row);
        }

        dataSet.numOfColumns = num;
        dataSet.types = types;
        dataSet.columns = protoDataSet.columns;
        dataSet.rows = rows;

        return dataSet;
    }

    function assignOptionalFields(dest, src, simpleFields, transformedFields) {
        simpleFields.forEach(function(field) {
            if(src[field] != null) {
                dest[field] = src[field];
            }
        })

        if(transformedFields) {
            for(var field in transformedFields) {
              if(src[field] != null) {
                var transformFunc = transformedFields[field];
                dest[field] = transformFunc(src[field]);
              }
            }
        }

        return dest;
    }

    var MetaDataFields = ['isMultiPart', 'contentType', 'size', 'seq', 'fileName', 'fileType', 'md5', 'description'];

    encodeMetaData = function(object) {
        return assignOptionalFields(MetaData.create(), object, MetaDataFields);
    }

    decodeMetaData = function(protoMetaData) {
        return assignOptionalFields({}, protoMetaData, MetaDataFields);
    }

    encodePropertyValue = function(object) {
        var type = encodeType(object.type),
            newPropertyValue = PropertyValue.create({
                "type" : type
            });

        if (object.isNull !== undefined && object.isNull !== null) {
            newPropertyValue.isNull = object.isNull;
        }

        setValue(type, object.value, newPropertyValue);

        return newPropertyValue;
    }

    decodePropertyValue = function(protoValue) {
        var propertyValue = {},
            protoType = protoValue.type,
            isNull = protoValue.isNull;

        if (isNull !== undefined && isNull !== null) {
            propertyValue.isNull = isNull;
        }

        propertyValue.type = decodeType(protoType);
        propertyValue.value = getValue(protoType, protoValue);

        return propertyValue;
    }

    encodePropertySet = function(object) {
        var keys = [],
            values = [];

        for (var key in object) {
            if (object.hasOwnProperty(key)) {
                keys.push(key);
                values.push(encodePropertyValue(object[key]))  
            }
        }

        return PropertySet.create({
            "keys" : keys, 
            "values" : values
        });
    }

    decodePropertySet = function(protoSet) {
        var propertySet = {},
            protoKeys = protoSet.keys,
            protoValues = protoSet.values;

        for (var i = 0; i < protoKeys.length; i++) {
            propertySet[protoKeys[i]] = decodePropertyValue(protoValues[i]);
        }

        return propertySet;
    }

    encodePropertySetList = function(object) {
        var propertySets = [];
        for (var i = 0; i < object.length; i++) {
            propertySets.push(encodePropertySet(object[i]));
        }
        return PropertySetList.create({
            "propertySet" : propertySets
        });
    }

    var decodePropertySetList = arrayTransform(decodePropertySet);

    encodeParameter = function(object) {
        var type = encodeType(object.type),
            newParameter = Parameter.create({
                "name" : object.name, 
                "type" : type
            });
        setValue(type, object.value, newParameter);
        return newParameter;
    }

    decodeParameter = function(protoParameter) {
        var protoType = protoParameter.type,
            parameter = {};

        parameter.name = protoParameter.name;
        parameter.type = decodeType(protoType);
        parameter.value = getValue(protoType, protoParameter);

        return parameter;
    }

    var TemplateFields = ['version', 'templateRef', 'isDefinition'];

    encodeTemplate = function(object) {
        return assignOptionalFields(Template.create(), object, TemplateFields, {
            metrics:    arrayTransform(encodeMetric),
            parameters: arrayTransform(encodeParameter)
        });
    }

    decodeTemplate = function(protoTemplate) {
        return assignOptionalFields({}, protoTemplate, TemplateFields, {
            metrics:    arrayTransform(decodeMetric),
            parameters: arrayTransform(decodeParameter)
        });
    }

    var MetricFields = ['alias', 'isHistorical', 'isTransient', 'isNull'];

    encodeMetric = function(metric) {
        var newMetric = Metric.create({
                "name" : metric.name
            }),
            value = metric.value,
            datatype = encodeType(metric.type);
        
        // Get metric type and value
        newMetric.datatype = datatype;
        setValue(datatype, value, newMetric);

        assignOptionalFields(newMetric, metric, MetricFields, {
          metadata: encodeMetaData,
          properties: encodePropertySet});
        return newMetric;
    }

    decodeMetric = function(protoMetric) {
        var metric = assignOptionalFields({}, protoMetric, MetricFields, {
          metadata: decodeMetaData,
          properties: decodePropertySet});

        metric.name = protoMetric.name;
        metric.type = decodeType(protoMetric.datatype);
        metric.value = getValue(protoMetric.datatype, protoMetric);
        return metric;
    }

    var PayloadFields = ['seq', 'uuid', 'body'];

    exports.encodePayload = function(object) {
        var payload = assignOptionalFields(Payload.create({timestamp: object.timestamp}), object, PayloadFields, {
            metrics: arrayTransform(encodeMetric)
        })
        return Payload.encode(payload).finish();
    }

    exports.decodePayload = function(proto) {
        var sparkplugPayload = Payload.decode(proto);
        var payload = assignOptionalFields({timestamp: sparkplugPayload.timestamp.toNumber()}, sparkplugPayload, PayloadFields, {
            metrics: arrayTransform(decodeMetric)
        })
        return payload;
    }
}());
