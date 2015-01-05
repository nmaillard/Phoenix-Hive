package org.apache.phoenix.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.hive.util.HiveConstants;
import org.apache.phoenix.hive.util.PhoenixConfigurationUtil;
import org.apache.phoenix.hive.util.ConnectionUtil;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;

/**
 * Hive specific InputFormat. Hive needs the mapred class and code base makes assumptions about the tables being actual files in HDFS
 * Extracting the table location to use a fake path.
 * Need to revisit this class and extend the common PhoenixInputFormat
 */

public class PhoenixInputFormat<T extends DBWritable> extends
        org.apache.hadoop.mapreduce.InputFormat<Text, T> implements
        org.apache.hadoop.mapred.InputFormat<NullWritable, T> {
    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);
    private Configuration configuration;
    private Connection connection;
    private QueryPlan queryPlan;

    public org.apache.hadoop.mapred.RecordReader<NullWritable, T> createRecordReader(
            org.apache.hadoop.mapred.InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        setConf(context.getConfiguration());
        QueryPlan queryPlan = getQueryPlan(context);

        Class inputClass = PhoenixConfigurationUtil.getInputClass(this.configuration);
        return new PhoenixRecordReader(inputClass, this.configuration, queryPlan);
    }

    /*
     * public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int numSplits) throws
     * IOException { System.out.println("PhoenixInputFormat getSplits with numsplits ");
     * setConf(jobConf); QueryPlan queryPlan = getQueryPlan(); List allSplits =
     * queryPlan.getSplits(); System.out.println("PhoenixInputFormat allSplits "+allSplits.size());
     * Path path = new Path(jobConf.get("location")); List splits = generateSplits(queryPlan,
     * allSplits,path); org.apache.hadoop.mapred.InputSplit[] asplits = new
     * org.apache.hadoop.mapred.InputSplit[splits.size()]; LOG.debug("Splits size " +
     * splits.size()); return (org.apache.hadoop.mapred.InputSplit[])splits.toArray(asplits); }
     */

    //
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {
        return null;
    }

    public FileSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        setConf(job);
        QueryPlan queryPlan = getQueryPlan();
        List allSplits = queryPlan.getSplits();
        Path path = new Path(job.get("location"));
        List splits = generateSplits(queryPlan, allSplits, path);
        FileSplit[] asplits = new FileSplit[splits.size()];
        LOG.debug("Splits size " + splits.size());
        splits.toArray(asplits);
        for (FileSplit fs : asplits) {
            System.out.println("iterate " + fs.toString());
        }
        return asplits;
    }

    private List<FileSplit> generateSplits(QueryPlan qplan, List<KeyRange> splits, Path path)
            throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        List psplits = Lists.newArrayListWithExpectedSize(splits.size());
        StatementContext context = qplan.getContext();
        TableRef tableRef = qplan.getTableRef();
        for (KeyRange split : splits) {
            Scan splitScan = new Scan(context.getScan());

            if (tableRef.getTable().getBucketNum() != null) {
                LOG.error("Salted/bucketed Tables not yet supported");
                throw new IOException("Salted/bucketed Tables not yet supported");
            }

            if (ScanUtil.intersectScanRange(splitScan, split.getLowerRange(),
                split.getUpperRange(), context.getScanRanges().useSkipScanFilter())) {
                PhoenixInputSplit inputSplit =
                        new PhoenixInputSplit(KeyRange.getKeyRange(splitScan.getStartRow(),
                            splitScan.getStopRow()), path);
                psplits.add(inputSplit);
            }
        }
        return psplits;
    }

    public org.apache.hadoop.mapred.RecordReader getRecordReader(
            org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        setConf(job);
        QueryPlan queryPlan = getQueryPlan();

        Class inputClass = PhoenixConfigurationUtil.getInputClass(this.configuration);
        PhoenixRecordReader r = new PhoenixRecordReader(inputClass, this.configuration, queryPlan);
        try {
            r.init(split);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return r;
    }

    public org.apache.hadoop.mapreduce.RecordReader<Text, T> createRecordReader(
            org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return null;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    private Connection getConnection() {
        try {
            if (this.connection == null) this.connection =
                    ConnectionUtil.getConnection(this.configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this.connection;
    }

    private QueryPlan getQueryPlan(JobContext context) throws IOException {
        Preconditions.checkNotNull(context);
        if (this.queryPlan == null) {
            try {
                Connection connection = getConnection();
                String selectStatement =
                        PhoenixConfigurationUtil.getSelectStatement(this.configuration);
                Preconditions.checkNotNull(selectStatement);
                Statement statement = connection.createStatement();
                PhoenixStatement pstmt =
                        (PhoenixStatement) statement.unwrap(PhoenixStatement.class);
                this.queryPlan = pstmt.compileQuery(selectStatement);
                this.queryPlan.iterator();
            } catch (Exception exception) {
                LOG.error(String.format("Failed to get the query plan with error [%s]",
                    new Object[] { exception.getMessage() }));
                throw new RuntimeException(exception);
            }
        }
        return this.queryPlan;
    }

    private QueryPlan getQueryPlan() throws IOException {
        try {
            LOG.debug("PhoenixInputFormat getQueryPlan statement "
                    + this.configuration.get("phoenix.select.stmt"));
            Connection connection = getConnection();
            String selectStatement =
                    PhoenixConfigurationUtil.getSelectStatement(this.configuration);
            Preconditions.checkNotNull(selectStatement);
            Statement statement = connection.createStatement();
            PhoenixStatement pstmt = (PhoenixStatement) statement.unwrap(PhoenixStatement.class);
            this.queryPlan = pstmt.compileQuery(selectStatement);
            this.queryPlan.iterator();
        } catch (Exception exception) {
            LOG.error(String.format("Failed to get the query plan with error [%s]",
                new Object[] { exception.getMessage() }));
            throw new RuntimeException(exception);
        }
        return this.queryPlan;
    }

    public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName,
            String conditions, String[] fieldNames) {
        job.setInputFormatClass(PhoenixInputFormat.class);
        Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
        PhoenixConfigurationUtil.setSelectColumnNames(configuration, fieldNames);
        PhoenixConfigurationUtil.setInputClass(configuration, inputClass);
        PhoenixConfigurationUtil.setSchemaType(configuration,
            PhoenixConfigurationUtil.SchemaType.TABLE);
    }

    public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName,
            String inputQuery) {
        job.setInputFormatClass(PhoenixInputFormat.class);
        Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
        PhoenixConfigurationUtil.setInputQuery(configuration, inputQuery);
        PhoenixConfigurationUtil.setInputClass(configuration, inputClass);
        PhoenixConfigurationUtil.setSchemaType(configuration,
            PhoenixConfigurationUtil.SchemaType.QUERY);
    }

    public static class NullDBWritable implements DBWritable, Writable {
        public void readFields(DataInput in) throws IOException {
        }

        public void readFields(ResultSet arg0) throws SQLException {
        }

        public void write(DataOutput out) throws IOException {
        }

        public void write(PreparedStatement arg0) throws SQLException {
        }
    }
}