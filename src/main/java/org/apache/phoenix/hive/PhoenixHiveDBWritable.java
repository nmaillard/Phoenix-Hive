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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ColumnInfo;

public class PhoenixHiveDBWritable implements Writable,DBWritable {
	private static final Log LOG = LogFactory.getLog(PhoenixHiveDBWritable.class);
	
	private final List<Object> values = new ArrayList<Object>();
    private PDataType[] PDataTypes = null;

	public PhoenixHiveDBWritable(){
		LOG.debug("PhoenixHiveDBWritable construt");
	}
	
	
	public PhoenixHiveDBWritable(PDataType[] categories) {
	    LOG.info("PDataTypes "+categories);
	    System.out.println("PDataTypes "+categories);
		this.PDataTypes = categories;
	}

	public void readFields(ResultSet arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	
	public void add(Object value) {
		values.add(value);
	}

	public void clear(){
		values.clear();
	}
	
	public void write(PreparedStatement statement) throws SQLException {
		LOG.info("PhoenixHiveDBWritable write");
 		for (int i = 0; i < values.size(); i++) {
 			Object o = values.get(i);
 				try {
                 if (o != null) {
                     LOG.info(" value " + o.toString() + " type " + PDataTypes[i].getSqlTypeName()+" int value "+PDataTypes[i].getSqlType());
                     statement.setObject(i + 1, PDataType.fromTypeId(PDataTypes[i].getSqlType()).toObject(o.toString()));
                 } else {
                     LOG.info(" value NULL  type " + PDataTypes[i].getSqlTypeName()+" int value "+PDataTypes[i].getSqlType());
                     statement.setNull(i + 1, PDataTypes[i].getSqlType());
                 }
             } catch (RuntimeException re) {
                 throw new RuntimeException(String.format("Unable to process column %s, innerMessage=%s"
                         ,re.getMessage()),re);
                 
             }
 		}
	}
	
	private Object convertTypeSpecificValue(Object o, byte type, Integer sqlType) {
        return PDataType.fromTypeId(sqlType).toObject(o.toString());
    }


	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}


	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
