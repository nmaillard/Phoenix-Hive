package org.apache.phoenix.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.Progressable;
import org.apache.phoenix.hive.util.PhoenixConfigurationUtil;
import org.apache.phoenix.hive.util.ConnectionUtil;

public class PhoenixOutputFormat<T extends DBWritable> extends
        org.apache.hadoop.mapreduce.OutputFormat<NullWritable, T> implements
        org.apache.hadoop.mapred.OutputFormat<NullWritable, T> {
    private static final Log LOG = LogFactory.getLog(PhoenixOutputFormat.class);
    private Connection connection;
    private Configuration config;

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new PhoenixOutputCommitter(this);
    }

    public org.apache.hadoop.mapreduce.RecordWriter<NullWritable, T> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        LOG.info("Hive Connection should not go through here");
        return null;
    }

    synchronized Connection getConnection(Configuration configuration) throws IOException {
        if (this.connection != null) {
            return this.connection;
        }

        this.config = configuration;
        try {
            LOG.info("Initializing new Phoenix connection...");
            this.connection = ConnectionUtil.getConnection(configuration);
            LOG.info("Initialized Phoenix connection, autoCommit="
                    + this.connection.getAutoCommit());
            return this.connection;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public static void setOutput(Job job, String tableName, String columns) {
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
        PhoenixConfigurationUtil.setUpsertColumnNames(configuration, columns);
    }

    public static void setOutput(Job job, String tableName, String[] fieldNames) {
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
        PhoenixConfigurationUtil.setUpsertColumnNames(configuration, fieldNames);
    }

    public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
        LOG.debug("PhoenixOutputFormat checkOutputSpecs");
    }

    public org.apache.hadoop.mapred.RecordWriter<NullWritable, T> getRecordWriter(FileSystem fs,
            JobConf conf, String st, Progressable progress) throws IOException {
        try {
            return new PhoenixRecordWriter(getConnection(conf), conf);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}