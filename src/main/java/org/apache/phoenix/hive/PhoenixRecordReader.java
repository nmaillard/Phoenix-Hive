package org.apache.phoenix.hive;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.ScanUtil;

public class PhoenixRecordReader<T extends DBWritable> extends
        org.apache.hadoop.mapreduce.RecordReader<NullWritable, T> implements
        org.apache.hadoop.mapred.RecordReader<NullWritable, T> {
    private static final Log LOG = LogFactory.getLog(PhoenixRecordReader.class);
    private final Configuration configuration;
    private final QueryPlan queryPlan;
    private NullWritable key = NullWritable.get();
    private T value = null;
    private Class<T> inputClass;
    private ResultIterator resultIterator = null;
    private PhoenixResultSet resultSet;

    public PhoenixRecordReader(Class<T> inputClass, Configuration configuration, QueryPlan queryPlan) {
        this.inputClass = inputClass;
        this.configuration = configuration;
        this.queryPlan = queryPlan;
    }

    public void close() throws IOException {
        if (this.resultIterator != null) try {
            this.resultIterator.close();
        } catch (SQLException e) {
            LOG.error(" Error closing resultset.");
        }
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    public T getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    public float getProgress() {
        return 0.0F;
    }

    public void
            initialize(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
        PhoenixInputSplit pSplit = (PhoenixInputSplit) split;
        KeyRange keyRange = pSplit.getKeyRange();
        Scan splitScan = this.queryPlan.getContext().getScan();
        Scan scan = new Scan(splitScan);
        ScanUtil.intersectScanRange(scan, keyRange.getLowerRange(), keyRange.getUpperRange(),
            this.queryPlan.getContext().getScanRanges().useSkipScanFilter());
        try {
            TableResultIterator tableResultIterator =
                    new TableResultIterator(this.queryPlan.getContext(),
                            this.queryPlan.getTableRef(), scan);
            if (this.queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) this.resultIterator =
                    new SequenceResultIterator(tableResultIterator, this.queryPlan.getContext()
                            .getSequenceManager());
            else {
                this.resultIterator = tableResultIterator;
            }
            this.resultSet =
                    new PhoenixResultSet(this.resultIterator, this.queryPlan.getProjector(),
                            this.queryPlan.getContext().getStatement());
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] initializing PhoenixRecordReader. ",
                new Object[] { e.getMessage() }));
            Throwables.propagate(e);
        }
    }

    public void init(org.apache.hadoop.mapred.InputSplit split) throws IOException,
            InterruptedException {
        PhoenixInputSplit pSplit = (PhoenixInputSplit) split;
        KeyRange keyRange = pSplit.getKeyRange();
        Scan splitScan = this.queryPlan.getContext().getScan();
        Scan scan = new Scan(splitScan);
        ScanUtil.intersectScanRange(scan, keyRange.getLowerRange(), keyRange.getUpperRange(),
            this.queryPlan.getContext().getScanRanges().useSkipScanFilter());
        try {
            TableResultIterator tableResultIterator =
                    new TableResultIterator(this.queryPlan.getContext(),
                            this.queryPlan.getTableRef(), scan);
            if (this.queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) this.resultIterator =
                    new SequenceResultIterator(tableResultIterator, this.queryPlan.getContext()
                            .getSequenceManager());
            else {
                this.resultIterator = tableResultIterator;
            }
            this.resultSet =
                    new PhoenixResultSet(this.resultIterator, this.queryPlan.getProjector(),
                            this.queryPlan.getContext().getStatement());
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] initializing PhoenixRecordReader. ",
                new Object[] { e.getMessage() }));
            Throwables.propagate(e);
        }
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.key == null) {
            this.key = NullWritable.get();
        }
        if (this.value == null) {
            this.value =
                    (T) ((DBWritable) ReflectionUtils.newInstance(this.inputClass,
                        this.configuration));
        }
        Preconditions.checkNotNull(this.resultSet);
        try {
            if (!this.resultSet.next()) {
                return false;
            }
            this.value.readFields(this.resultSet);

            return true;
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] occurred while iterating over the resultset. ",
                new Object[] { e.getMessage() }));
            Throwables.propagate(e);
        }
        return false;
    }

    public boolean next(NullWritable key, T val) throws IOException {
        if (key == null) {
            key = NullWritable.get();
        }
        if (this.value == null) {
            this.value =
                    (T) ((DBWritable) ReflectionUtils.newInstance(this.inputClass,
                        this.configuration));
        }
        Preconditions.checkNotNull(this.resultSet);
        try {
            if (!this.resultSet.next()) {
                return false;
            }
            this.value.readFields(this.resultSet);
            LOG.debug("PhoenixRecordReader resultset size" + this.resultSet.getFetchSize());
            return true;
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] occurred while iterating over the resultset. ",
                new Object[] { e.getMessage() }));
            Throwables.propagate(e);
        }
        return false;
    }

    public NullWritable createKey() {
        return this.key;
    }

    public T createValue() {
        this.value =
                (T) ((DBWritable) ReflectionUtils.newInstance(this.inputClass, this.configuration));
        return this.value;
    }

    public long getPos() throws IOException {
        return 0L;
    }
}