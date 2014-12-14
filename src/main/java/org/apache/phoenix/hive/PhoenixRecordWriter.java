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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.hive.util.PhoenixConfigurationUtil;

public class PhoenixRecordWriter<T extends DBWritable> implements RecordWriter<NullWritable, T> {
    
    private static final Log LOG = LogFactory.getLog(PhoenixRecordWriter.class);
    
    private long numRecords = 0;
    
    private final Connection conn;
    private final PreparedStatement statement;
    private final long batchSize;
    
    public PhoenixRecordWriter(Connection conn, final Configuration config) throws SQLException {
        this.conn = conn;
        this.batchSize = PhoenixConfigurationUtil.getBatchSize(config);
        final String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(config);
        this.statement = this.conn.prepareStatement(upsertQuery);
    }


    public void write(NullWritable n, T record) throws IOException {      
    	LOG.info("PhoenixRecordWriter write ");
        try {
            record.write(statement);
            numRecords++;
            statement.addBatch();
            
            if (numRecords % batchSize == 0) {
                LOG.info("commit called on a batch of size : " + batchSize);
                statement.executeBatch();
                conn.commit();
            }
            LOG.info("numRecords is  "+numRecords);
        } catch (SQLException e) {
            throw new IOException("Exception while committing to database.", e);
        }
    }


	public void close(Reporter arg0) throws IOException {
	    LOG.info("PhoenixRecordWriter close with reporter ");
        try {
            statement.executeBatch();
            conn.commit();
          } catch (SQLException e) {
            try {
                conn.rollback();
            }
            catch (SQLException ex) {
                throw new IOException("Exception while closing the connection", e);
            }
            throw new IOException(e.getMessage());
          } finally {
            try {
              statement.close();
              conn.close();
            }
            catch (SQLException ex) {
              throw new IOException(ex.getMessage());
            }
          }
		
	}


}
