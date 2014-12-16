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
package org.apache.phoenix.hive;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.phoenix.hive.util.ConfigurationUtil;
import org.apache.phoenix.hive.util.ConnectionUtil;
import org.apache.phoenix.hive.util.HiveTypeUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;

import com.google.common.base.Splitter;

/**
 * Executes logic on the create and delepath on Hive execution engine
 */

public class PhoenixMetaHook implements HiveMetaHook {
    static Log LOG = LogFactory.getLog(PhoenixMetaHook.class.getName());

    public PhoenixMetaHook() {
        super();
    }
    
    /**
     * This method contains all the logic for creating Phoenix tables from Hive Create cmd
     * anaged tables are always created through Hive, External tables need to check for prexisiting table and schemas
     * @param tbl all table properties
     */

    public void commitCreateTable(Table tbl) throws MetaException {
        LOG.debug("PhoenixMetaHook commitCreateTable ");
        Map<String, String> fields = new LinkedHashMap<String, String>();
        Map<String, String> mps = tbl.getParameters();
        String fname;
        /*Find Phoenix Table Name, if none supplied default back to hive table name*/
        String tablename =
                (mps.get(ConfigurationUtil.TABLE_NAME) != null) ? mps
                        .get(ConfigurationUtil.TABLE_NAME) : tbl.getTableName();
        
       /*Extract Hive to Phoenix column mappings*/
       String mapping = mps.get(ConfigurationUtil.COLUMN_MAPPING);
        Map<String, String> mappings = null;
        if (mapping != null && mapping.length() > 0) {
            mapping = mapping.toLowerCase();
            mappings =
                    Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator(":")
                            .split(mapping);
        }
        /*Synchronise Hive columns with Phoenix mappings*/
        for (FieldSchema fs : tbl.getSd().getCols()) {
            try {
                fname = fs.getName().toLowerCase();
                if (mappings != null) {
                    fname =
                            mappings.get(fname) == null ? fs.getName().toLowerCase() : mappings
                                    .get(fname);
                }
                fields.put(fname, HiveTypeUtil.HiveType2PDataType(fs.getType()).toString());
            } catch (SerDeException e) {
                e.printStackTrace();
            }
        }
        /*Extract Phoenix primary Keys, they are mandatory*/
        String pk = mps.get(ConfigurationUtil.PHOENIX_ROWKEYS);
        if (pk == null || pk.length() == 0) {
            throw new MetaException("Phoenix Table no Rowkeys specified in "
                    + ConfigurationUtil.PHOENIX_ROWKEYS);
        }
        
        int salt_buckets = 0;
        String salting =  mps.get(ConfigurationUtil.SALT_BUCKETS);
        
        if (salting != null && salting.length() > 0) {
            try{
                salt_buckets = Integer.parseInt(salting);
                if(salt_buckets>256){
                    LOG.warn("Salt Buckets should be between 1-256 we will cap at 256");
                    salt_buckets = 256;
                }
                if(salt_buckets<0){
                    LOG.warn("Salt Buckets should be between 1-256 we will undercap at 0");
                    salt_buckets = 256;
                }
            }catch(NumberFormatException nfe){
                salt_buckets = 0;
            }
        }
        String compression  = mps.get(ConfigurationUtil.COMPRESSION);
        if(compression!=null &&  compression.equalsIgnoreCase("gz")){
            compression = "GZ";
        }else{
            compression = null;
        }
        /*Creating phoenix Table for the user if specified*/
        try {
        	Connection conn;
        	conn = ConnectionUtil.getConnection(tbl);
        	/*If managed table */
            if (tbl.getTableType().equals(TableType.MANAGED_TABLE.name())) {
                if (PhoenixUtil.findTable(conn, tablename)) {
                    throw new MetaException(
                            " Phoenix table already exists cannot create use EXTERNAL");
                } else {

                    PhoenixUtil.createTable(conn, tablename, fields, pk.split(","), false,salt_buckets,compression);
                }
            }
            /*If external table test if schemas coincide create only if autocreate*/
            else if (tbl.getTableType().equals(TableType.EXTERNAL_TABLE.name())){
                if (PhoenixUtil.findTable(conn, tablename)) {
                    LOG.info("CREATE External table table already exists");
                    PhoenixUtil.testTable(conn, tablename, fields);
                }else if (tbl.getParameters().get(ConfigurationUtil.AUTOCREATE) != null&& tbl.getParameters().get(ConfigurationUtil.AUTOCREATE).equalsIgnoreCase("true")){
                    PhoenixUtil.createTable(conn,tablename,fields, pk.split(","), false,salt_buckets,compression);
                }
            }else{
                throw new MetaException(" Phoenix Unsupported table Type: " + tbl.getTableType());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new MetaException(" Phoenix table creation SQLException: " + e.getMessage());
        }

    }
    
 
    public void commitDropTable(Table tbl, boolean bool) throws MetaException {
        Map<String, String> mps = tbl.getParameters();
        /*If no table name properties is specified use the Hive tablename*/
        String tablename =
                (mps.get(ConfigurationUtil.TABLE_NAME) != null) ? mps
                        .get(ConfigurationUtil.TABLE_NAME) : tbl.getTableName();
        try {
            Connection conn;
            /*Dropping Hive table also drops phoenix table if managed or if autodrop is specified*/
            if (tbl.getTableType().equals(TableType.MANAGED_TABLE.name())) {
            	conn = ConnectionUtil.getConnection(tbl);
            	PhoenixUtil.dropTable(conn,tablename);
            }
            if (tbl.getTableType().equals(TableType.EXTERNAL_TABLE.name())
                    && tbl.getParameters().get(ConfigurationUtil.AUTODROP) != null
                    && tbl.getParameters().get(ConfigurationUtil.AUTODROP).equalsIgnoreCase("true")) {
            	conn = ConnectionUtil.getConnection(tbl);
            	PhoenixUtil.dropTable(conn,tablename);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new MetaException("Phoenix table drop SQLException: " + e.getMessage());
        }
    }

    public void preCreateTable(Table tbl) throws MetaException {
        // If any prior creating logic is useful
    }

    public void preDropTable(Table tbl) throws MetaException {
     // If any prior creating logic is useful

    }

    public void rollbackCreateTable(Table tbl) throws MetaException {
        // TODO Auto-generated method stub
    }

    public void rollbackDropTable(Table tbl) throws MetaException {
        // TODO Auto-generated method stub
    }

}
