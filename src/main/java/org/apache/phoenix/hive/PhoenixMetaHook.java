package org.apache.phoenix.hive;

import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.phoenix.hive.util.ConnectionUtil;
import org.apache.phoenix.hive.util.HiveTypeUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.schema.PDataType;

public class PhoenixMetaHook
  implements HiveMetaHook
{
  static Log LOG = LogFactory.getLog(PhoenixMetaHook.class.getName());

  //Too much logic in this function must revisit and dispatch
  public void commitCreateTable(Table tbl)
    throws MetaException
  {
    LOG.debug("PhoenixMetaHook commitCreateTable ");
    Map fields = new LinkedHashMap();
    Map mps = tbl.getParameters();

    String tablename = mps.get("phoenix.hbase.table.name") != null ? (String)mps.get("phoenix.hbase.table.name") : tbl.getTableName();

    String mapping = (String)mps.get("phoenix.column.mapping");
    Map mappings = null;
    if ((mapping != null) && (mapping.length() > 0)) {
      mapping = mapping.toLowerCase();
      mappings = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator(":").split(mapping);
    }

    for (FieldSchema fs : tbl.getSd().getCols()) {
      try {
        String fname = fs.getName().toLowerCase();
        if (mappings != null) {
          fname = mappings.get(fname) == null ? fs.getName().toLowerCase() : (String)mappings.get(fname);
        }

        fields.put(fname, HiveTypeUtil.HiveType2PDataType(fs.getType()).toString());
      } catch (SerDeException e) {
        e.printStackTrace();
      }
    }

    String pk = (String)mps.get("phoenix.rowkeys");
    if ((pk == null) || (pk.length() == 0)) {
      throw new MetaException("Phoenix Table no Rowkeys specified in phoenix.rowkeys");
    }

    int salt_buckets = 0;
    String salting = (String)mps.get("saltbuckets");

    if ((salting != null) && (salting.length() > 0)) {
      try {
        salt_buckets = Integer.parseInt(salting);
        if (salt_buckets > 256) {
          LOG.warn("Salt Buckets should be between 1-256 we will cap at 256");
          salt_buckets = 256;
        }
        if (salt_buckets < 0) {
          LOG.warn("Salt Buckets should be between 1-256 we will undercap at 0");
          salt_buckets = 0;
        }
      } catch (NumberFormatException nfe) {
        salt_buckets = 0;
      }
    }
    String compression = (String)mps.get("compression");
    if ((compression != null) && (compression.equalsIgnoreCase("gz")))
      compression = "GZ";
    else {
      compression = null;
    }

    try
    {
      Connection conn = ConnectionUtil.getConnection(tbl);

      if (tbl.getTableType().equals(TableType.MANAGED_TABLE.name())) {
        if (PhoenixUtil.findTable(conn, tablename)) {
          throw new MetaException(" Phoenix table already exists cannot create use EXTERNAL");
        }

        PhoenixUtil.createTable(conn, tablename, fields, pk.split(","), false, salt_buckets, compression);
      }
      else if (tbl.getTableType().equals(TableType.EXTERNAL_TABLE.name())) {
        if (PhoenixUtil.findTable(conn, tablename)) {
          LOG.info("CREATE External table table already exists");
          PhoenixUtil.testTable(conn, tablename, fields);
        } else if ((tbl.getParameters().get("autocreate") != null) && (((String)tbl.getParameters().get("autocreate")).equalsIgnoreCase("true"))) {
          PhoenixUtil.createTable(conn, tablename, fields, pk.split(","), false, salt_buckets, compression);
        }
      } else {
        throw new MetaException(" Phoenix Unsupported table Type: " + tbl.getTableType());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new MetaException(" Phoenix table creation SQLException: " + e.getMessage());
    }
  }

  public void commitDropTable(Table tbl, boolean bool)
    throws MetaException
  {
    Map mps = tbl.getParameters();

    String tablename = mps.get("phoenix.hbase.table.name") != null ? (String)mps.get("phoenix.hbase.table.name") : tbl.getTableName();
    try
    {
      if (tbl.getTableType().equals(TableType.MANAGED_TABLE.name())) {
        Connection conn = ConnectionUtil.getConnection(tbl);
        PhoenixUtil.dropTable(conn, tablename);
      }
      if ((tbl.getTableType().equals(TableType.EXTERNAL_TABLE.name())) && (tbl.getParameters().get("autodrop") != null) && (((String)tbl.getParameters().get("autodrop")).equalsIgnoreCase("true")))
      {
        Connection conn = ConnectionUtil.getConnection(tbl);
        PhoenixUtil.dropTable(conn, tablename);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new MetaException("Phoenix table drop SQLException: " + e.getMessage());
    }
  }

  public void preCreateTable(Table tbl)
    throws MetaException
  {
  }

  public void preDropTable(Table tbl)
    throws MetaException
  {
  }

  public void rollbackCreateTable(Table tbl)
    throws MetaException
  {
  }

  public void rollbackDropTable(Table tbl)
    throws MetaException
  {
  }
}