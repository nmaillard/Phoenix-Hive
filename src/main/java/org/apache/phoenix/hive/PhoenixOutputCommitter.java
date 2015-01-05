package org.apache.phoenix.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PhoenixOutputCommitter extends OutputCommitter
{
  public final Log LOG = LogFactory.getLog(PhoenixOutputCommitter.class);

  public PhoenixOutputCommitter(PhoenixOutputFormat outputFormat)
  {
  }

  public void abortTask(TaskAttemptContext context)
    throws IOException
  {
  }

  public void commitTask(TaskAttemptContext context)
    throws IOException
  {
  }

  public boolean needsTaskCommit(TaskAttemptContext context)
    throws IOException
  {
    return true;
  }

  public void setupJob(JobContext jobContext)
    throws IOException
  {
  }

  public void setupTask(TaskAttemptContext context)
    throws IOException
  {
  }

  public void commit(Connection connection)
    throws IOException
  {
    try
    {
      if ((connection == null) || (connection.isClosed()))
        throw new IOException("Trying to commit a connection that is null or closed: " + connection);
    }
    catch (SQLException e) {
      throw new IOException("Exception calling isClosed on connection", e);
    }
    try
    {
      this.LOG.info("Commit called on task completion");
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Exception while trying to commit a connection. ", e);
    } finally {
      try {
        this.LOG.info("Closing connection to database on task completion");
        connection.close();
      } catch (SQLException e) {
        this.LOG.warn("Exception while trying to close database connection", e);
      }
    }
  }
}