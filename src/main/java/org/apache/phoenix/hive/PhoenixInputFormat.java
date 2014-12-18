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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.hive.util.PhoenixHiveConfiguration;

/**
 * Created by nmaillard on 6/23/14.
 */
public class PhoenixInputFormat extends HiveInputFormat<NullWritable, PhoenixHiveDBWritable> {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);
    private PhoenixHiveConfiguration phoenixConfiguration;
    private Connection connection;
    private QueryPlan queryPlan;

    private boolean jobConfSet = false;

    /**
     * instantiated by framework
     */
    public PhoenixInputFormat() {
        LOG.info("Phoenix InputF init");
    }

    @Override
    public RecordReader<NullWritable, PhoenixHiveDBWritable> getRecordReader(InputSplit split,
            JobConf jobConf, Reporter reporter) {
        if (!jobConfSet) {
            super.configure(jobConf);
            this.jobConfSet = true;
        }
        //return new PhoenixHiveDBWritable();
        return (RecordReader<NullWritable, PhoenixHiveDBWritable>) new PhoenixRecordReader(PhoenixHiveDBWritable.class,jobConf, null);
        // return new PhoenixRecordReader(jobConf,queryPlan);
        //return null;
        // return new PhoenixRecordReader();
    }

    /**
     * Determine InputSplits for the Azure Table reader, using the partition keys given in the table
     * definition as the input split boundaries.
     * @param jobConf The Hadoop job configuration
     * @param chunks The desired number of splits, which is pretty much ignored at the moment. This
     *            is not ideal, since there should be a secondary mechanism to further split a table
     *            partition
     */

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int chunks) throws IOException {
        if (!jobConfSet) {
            super.configure(jobConf);
            this.jobConfSet = true;
        }
        //InputSplit[] t = PhoenixInputSplit.getSplits(jobConf, chunks);
        // /System.out.println("ret splits "+t.length);

        // return t;
        // return super.getSplits(jobConf, chunks);
        // InputSplit [] results = new InputSplit[chunks];
        // System.out.println("splits length "+results.length);
        /*
         * PhoenixHiveSplit[] splits = new PhoenixHiveSplit[chunks]; for (int i = 0; i < chunks;
         * i++) { if ((i + 1) == chunks) { splits[i] = new PhoenixHiveSplit(); } else { splits[i] =
         * new PhoenixHiveSplit(); } } return splits;
         */
        //PhoenixSplit[] splits = new PhoenixSplit[1];
        //splits[0] = new PhoenixSplit();
        return null;
    }

    /**
     * Returns the query plan associated with the select query.
     * @param context
     * @return
     * @throws IOException
     * @throws SQLException
     */
    /*
     * private QueryPlan getQueryPlan(final JobConf jobConf) throws IOException {
     * Preconditions.checkNotNull(jobConf); if(queryPlan == null) { try{ PhoenixTable pt =
     * PhoenixTable.getInstance(jobConf); final Connection connection = pt.getConnection(); final
     * String selectStatement = getConf().getSelectStatement();
     * Preconditions.checkNotNull(selectStatement); final Statement statement =
     * connection.createStatement(); final PhoenixStatement pstmt =
     * statement.unwrap(PhoenixStatement.class); this.queryPlan =
     * pstmt.compileQuery(selectStatement); } catch(Exception exception) {
     * LOG.error(String.format("Failed to get the query plan with error [%s]"
     * ,exception.getMessage())); throw new RuntimeException(exception); } } return queryPlan; }
     */

    static class PhoenixHiveSplit extends FileSplit {
        InputSplit delegate;
        private Path path;

        PhoenixHiveSplit() {
            //this(new PhoenixInputSplit(), null);
        }

        PhoenixHiveSplit(InputSplit delegate, Path path) {
            super(path, 0, 0, (String[]) null);
            this.delegate = delegate;
            this.path = path;
        }

        @Override
        public long getLength() {
            // TODO: can this be delegated?
            return 1L;
        }

        @Override
        public String[] getLocations() throws IOException {
            return delegate.getLocations();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, path.toString());
            delegate.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            path = new Path(Text.readString(in));
            delegate.readFields(in);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }
}