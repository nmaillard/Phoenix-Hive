/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.hive.util.ConfigurationUtil;
import org.apache.phoenix.hive.util.HiveConstants;
import org.apache.phoenix.hive.util.HiveTypeUtil;
import org.apache.phoenix.schema.PDataType;

public class PhoenixSerde implements SerDe {
    static Log LOG = LogFactory.getLog(PhoenixSerde.class.getName());

    //private PhoenixRecord precord;
    private PhoenixHiveDBWritable phrecord;
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;
    private ObjectInspector ObjectInspector;
    private int fieldCount;
    private List<Object> row;


    private List<ObjectInspector> fieldOIs;
    
    /**
     * Serde Initialization class
     * returned by <a href="#getSerializedClass">getSerializedClass</a>
     * @param conf all keyvalues related to this job
     * @param tblprops all properties found in the table properties part
     * @throws SerDeException
     */

    public void initialize(Configuration conf, Properties tblProps) throws SerDeException {
        columnNames = Arrays.asList(tblProps.getProperty(HiveConstants.COLUMNS).split(","));
        columnTypes =
                TypeInfoUtils.getTypeInfosFromTypeString(tblProps
                        .getProperty(HiveConstants.COLUMNS_TYPES));
        LOG.debug("columnNames: "+columnNames);
        LOG.debug("columnTypes: "+columnNames);
        this.fieldCount = columnTypes.size();
        PDataType[] types = HiveTypeUtil.hiveTypesToSqlTypes(columnTypes);
        phrecord = new PhoenixHiveDBWritable(types);
        fieldOIs = new ArrayList<ObjectInspector>(columnNames.size());
        
        for (TypeInfo typeInfo : columnTypes) {
            fieldOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo));
        }
       
        this.ObjectInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldOIs);
        row = new ArrayList<Object>(columnNames.size());
    }
    
    /**
     * Hive will call this to deserialize a writbale  object. Returns an object of the same class
     * returned by <a href="#getSerializedClass">getSerializedClass</a>
     * @param row The object to serialize
     * @param inspector The ObjectInspector that knows about the object's structure
     * @return a serialized object 
     * @throws SerDeException
     */
    public Object deserialize(Writable wr) throws SerDeException {
        /*try {
            row.clear();
            final Text t = new Text();
            final PhoenixRecord pr = (PhoenixRecord) wr;
            for (int i = 0; i < fieldCount; i++) {
                t.set(columnNames.get(i));
                final Object value = pr.get(columnNames.get(i));
                if (value != null) {
                    row.add(value.toString());
                } else {
                    row.add(null);
                }

            }
            return row;
        } catch (Exception e) {
            throw new SerDeException();
        }*/
        throw new SerDeException("The Read Path is not yet usable, To read please use the standard Phoenix client");
        //return null;
    }
    
    /**
     * Hive need to access the standard ObjectInspector we have built in the init
     * @return the ObjectInspector 
     * @throws SerDeException
     */
    public ObjectInspector getObjectInspector() throws SerDeException {
        return ObjectInspector;
    }

    public SerDeStats getSerDeStats() {
        // TODO work stats into the process
        return null;
    }

    public Class<? extends Writable> getSerializedClass() {
        return PhoenixHiveDBWritable.class;
    }

    /**
     * Hive will call this to serialize an object. Returns a writable object of the same class
     * returned by <a href="#getSerializedClass">getSerializedClass</a>
     * @param row The object to serialize
     * @param inspector The ObjectInspector that knows about the object's structure
     * @return a serialized object in form of a Writable. Must be the same type returned by <a
     *         href="#getSerializedClass">getSerializedClass</a>
     * @throws SerDeException
     */
    public Writable serialize(Object row, ObjectInspector inspector) throws SerDeException {
        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
      
        if (fields.size() != fieldCount) {
            throw new SerDeException(String.format("Required %d columns, received %d.", fieldCount,
                fields.size()));
        }
        phrecord.clear();
        for (int i = 0; i < fieldCount; i++) {
            StructField structField = fields.get(i);
            if (structField != null) {
                Object field = structInspector.getStructFieldData(row, structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();
                switch (fieldOI.getCategory()) {
                case PRIMITIVE:
                    Writable value =(Writable)((PrimitiveObjectInspector) fieldOI).getPrimitiveWritableObject(field);
                    phrecord.add(value);
                    break;
                default:
                    //TODO add support for Array
                    new SerDeException("Phoenix Unsupported column type: " + fieldOI.getCategory());
                }
            }
        }

        return phrecord;
    }

}
