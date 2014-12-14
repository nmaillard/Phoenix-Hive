/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PDataType;

import com.google.common.base.Preconditions;

/**
 *
 */
public class ConfigurationUtil {

    public static final String TABLE_NAME = "phoenix.hbase.table.name";
    public static final String ZOOKEEPER_QUORUM = "phoenix.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT = "phoenix.zookeeper.client.port";
    public static final String ZOOKEEPER_PARENT = "phoenix.zookeeper.znode.parent";
    public static final String ZOOKEEPER_QUORUM_DEFAULT = "localhost";
    public static final String ZOOKEEPER_PORT_DEFAULT = "2181";
    public static final String ZOOKEEPER_PARENT_DEFAULT = "/hbase-unsecure";

    public static final String UPSERT_BATCH_SIZE = "phoenix.upsert.batch.size";
    public static final String COLUMN_MAPPING = "phoenix.column.mapping";
    public static final String AUTOCREATE = "autocreate";
    public static final String AUTODROP = "autodrop";
    public static final String AUTOCOMMIT = "autocommit";
    public static final String PHOENIX_ROWKEYS = "phoenix.rowkeys";
    public static final String SALT_BUCKETS = "saltbuckets";
    public static final String COMPRESSION = "compression";
    public static final String SPLIT = "split";
    public static final long DEFAULT_UPSERT_BATCH_SIZE = 1000;
    public static final String REDUCE_SPECULATIVE_EXEC =
            "mapred.reduce.tasks.speculative.execution";
    public static final String MAP_SPECULATIVE_EXEC = "mapred.map.tasks.speculative.execution";

    public static void setProperties(Properties tblProps, Map<String, String> jobProperties) {
        System.out.println("quorum:" + tblProps.getProperty(ConfigurationUtil.ZOOKEEPER_QUORUM));
        System.out.println("port:" + tblProps.getProperty(ConfigurationUtil.ZOOKEEPER_PORT));
        System.out.println("parent:" + tblProps.getProperty(ConfigurationUtil.ZOOKEEPER_PARENT));
        System.out.println("table:" + tblProps.getProperty(ConfigurationUtil.TABLE_NAME));
        System.out.println("batch:" + tblProps.getProperty(ConfigurationUtil.UPSERT_BATCH_SIZE));
        for (Entry<Object, Object> e : tblProps.entrySet()) {
            System.out.println("entry:" + e.getKey().toString() + " vlue "
                    + e.getValue().toString());
        }
        jobProperties.put(ConfigurationUtil.ZOOKEEPER_QUORUM, tblProps.getProperty(
            ConfigurationUtil.ZOOKEEPER_QUORUM, ConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT));
        jobProperties.put(ConfigurationUtil.ZOOKEEPER_PORT, tblProps.getProperty(
            ConfigurationUtil.ZOOKEEPER_PORT, ConfigurationUtil.ZOOKEEPER_PORT_DEFAULT));
        jobProperties.put(ConfigurationUtil.ZOOKEEPER_PARENT, tblProps.getProperty(
            ConfigurationUtil.ZOOKEEPER_PARENT, ConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT));
        String tableName = tblProps.getProperty(ConfigurationUtil.TABLE_NAME);
        if (tableName == null) {
            tableName = tblProps.get("name").toString();
            System.out.println("table name to string "+tableName);
            System.out.println("table name to string size"+tableName.split(".").length);
            tableName = tableName.split(".")[1];
        }
        jobProperties.put(ConfigurationUtil.TABLE_NAME, tableName);
    }
    

    public static PDataType[] hiveTypesToPDataType(
            PrimitiveObjectInspector.PrimitiveCategory[] hiveTypes) throws SerDeException {
        final PDataType[] result = new PDataType[hiveTypes.length];
        for (int i = 0; i < hiveTypes.length; i++) {
            result[i] = hiveTypeToPDataType(hiveTypes[i]);
        }
        return result;
    }

    public static PDataType[] hiveTypesToSqlTypes(List<TypeInfo> columnTypes) throws SerDeException {
        final PDataType[] result = new PDataType[columnTypes.size()];
        for (int i = 0; i < columnTypes.size(); i++) {
            result[i] = hiveTypeToPDataType(columnTypes.get(i));
        }
        return result;
    }

    /**
     * This method returns the most appropriate PDataType associated with the incoming hive type.
     * @param hiveType
     * @return PDataType
     */

    public static PDataType hiveTypeToPDataType(TypeInfo hiveType) throws SerDeException {
        System.out.println(" ConfigurstionUtil hiveTypeToPDataType");
        System.out.println(" typeInfo name " + hiveType.getTypeName());
        System.out.println("cat name " + hiveType.getCategory().name());
        switch (hiveType.getCategory()) {
        case PRIMITIVE:
            // return
            // hiveTypeToPDataType(((PrimitiveObjectInspector)hiveType).getPrimitiveCategory());
            return hiveTypeToPDataType(hiveType.getTypeName());
        default:

            throw new SerDeException("Unsupported column type: " + hiveType.getCategory().name());
        }
    }

    public static PDataType hiveTypeToPDataType(String hiveType) throws SerDeException {
        System.out.println(" ConfigurstionUtil hiveTypeToPDataType");
        final String lctype = hiveType.toLowerCase();
        if ("string".equals(lctype)) {
            return PDataType.VARCHAR;
        } else if ("float".equals(lctype)) {
            return PDataType.FLOAT;
        } else if ("double".equals(lctype)) {
            return PDataType.DOUBLE;
        } else if ("boolean".equals(lctype)) {
            return PDataType.BOOLEAN;
        } else if ("tinyint".equals(lctype)) {
            return PDataType.SMALLINT;
        } else if ("smallint".equals(lctype)) {
            return PDataType.SMALLINT;
        } else if ("int".equals(lctype)) {
            return PDataType.INTEGER;
        } else if ("bigint".equals(lctype)) {
            return PDataType.DOUBLE;
        } else if ("timestamp".equals(lctype)) {
            return PDataType.TIMESTAMP;
        } else if ("binary".equals(lctype)) {
            return PDataType.BINARY;
        } else if ("date".equals(lctype)) {
            return PDataType.DATE;
        } else if ("array".equals(lctype)) {
            // return PArrayDataType
        }
        throw new SerDeException("Unrecognized column type: " + hiveType);
    }

    /**
     * This method returns the most appropriate PDataType associated with the incoming hive type.
     * @param PrimitiveCategory
     * @return PDataType
     */

    public static PDataType
            hiveTypeToPDataType(PrimitiveObjectInspector.PrimitiveCategory hiveType)
                    throws SerDeException {
        /* TODO check backward type compatibility prior to hive 0.12 */

        if (hiveType == null) {
            return null;
        }
        switch (hiveType) {
        case BOOLEAN:
            return PDataType.BOOLEAN;
        case BYTE:
            return PDataType.BINARY;
        case DATE:
            return PDataType.DATE;
        case DECIMAL:
            return PDataType.DECIMAL;
        case DOUBLE:
            return PDataType.DOUBLE;
        case FLOAT:
            return PDataType.FLOAT;
        case INT:
            return PDataType.INTEGER;
        case LONG:
            return PDataType.LONG;
        case SHORT:
            return PDataType.SMALLINT;
        case STRING:
            return PDataType.VARCHAR;
        case TIMESTAMP:
            return PDataType.TIMESTAMP;
        case VARCHAR:
            return PDataType.VARCHAR;
        case VOID:
            return PDataType.CHAR;
        case UNKNOWN:
            throw new RuntimeException("Unknown primitive");
        default:
            new SerDeException("Unrecognized column type: " + hiveType);
        }
        return null;
    }
    

    /**
     * This method encodes a value with Phoenix data type. It begins with checking whether an object
     * is BINARY and makes a call to {@link #castBytes(Object, PDataType)} to convert bytes to
     * targetPhoenixType
     * @param o
     * @param targetPhoenixType
     * @return Object
     */
    public static Object castHiveTypeToPhoenix(Object o, byte objectType,
            PDataType targetPhoenixType) {
        /*
         * PDataType inferredPType = getType(o, objectType); if (inferredPType == null) { return
         * null; } if (inferredPType == PDataType.VARBINARY && targetPhoenixType !=
         * PDataType.VARBINARY) { try { o = castBytes(o, targetPhoenixType); inferredPType =
         * getType(o, DataType.findType(o)); } catch (IOException e) { throw new
         * RuntimeException("Error while casting bytes for object " + o); } } if (inferredPType ==
         * PDataType.DATE) { int inferredSqlType = targetPhoenixType.getSqlType(); if
         * (inferredSqlType == Types.DATE) { return new Date(((DateTime)o).getMillis()); } if
         * (inferredSqlType == Types.TIME) { return new Time(((DateTime)o).getMillis()); } if
         * (inferredSqlType == Types.TIMESTAMP) { return new Timestamp(((DateTime)o).getMillis()); }
         * } if (targetPhoenixType == inferredPType ||
         * inferredPType.isCoercibleTo(targetPhoenixType)) { return inferredPType.toObject(o,
         * targetPhoenixType); } throw new RuntimeException(o.getClass().getName() +
         * " cannot be coerced to " + targetPhoenixType.toString());
         */
        return null;
    }

    /**
     * This method converts bytes to the target type required for Phoenix. It uses
     * {@link Utf8StorageConverter} for the conversion.
     * @param o
     * @param targetPhoenixType
     * @return Object
     * @throws IOException
     */
    /*
     * public static Object castBytes(Object o, PDataType targetPhoenixType) throws IOException {
     * byte[] bytes = ((DataByteArray)o).get(); switch (targetPhoenixType) { case CHAR: case
     * VARCHAR: return utf8Converter.bytesToCharArray(bytes); case UNSIGNED_SMALLINT: case SMALLINT:
     * return utf8Converter.bytesToInteger(bytes).shortValue(); case UNSIGNED_TINYINT: case TINYINT:
     * return utf8Converter.bytesToInteger(bytes).byteValue(); case UNSIGNED_INT: case INTEGER:
     * return utf8Converter.bytesToInteger(bytes); case BOOLEAN: return
     * utf8Converter.bytesToBoolean(bytes); case DECIMAL: return
     * utf8Converter.bytesToBigDecimal(bytes); case FLOAT: case UNSIGNED_FLOAT: return
     * utf8Converter.bytesToFloat(bytes); case DOUBLE: case UNSIGNED_DOUBLE: return
     * utf8Converter.bytesToDouble(bytes); case UNSIGNED_LONG: case LONG: return
     * utf8Converter.bytesToLong(bytes); case TIME: case TIMESTAMP: case DATE: case UNSIGNED_TIME:
     * case UNSIGNED_TIMESTAMP: case UNSIGNED_DATE: return utf8Converter.bytesToDateTime(bytes);
     * default: return o; } }
     */
}