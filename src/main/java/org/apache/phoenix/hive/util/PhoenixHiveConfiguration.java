package org.apache.phoenix.hive.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.base.Preconditions;

/**
 * Created by nmaillard on 6/23/14.
 */
public class PhoenixHiveConfiguration {
    private static final Log LOG = LogFactory.getLog(PhoenixHiveConfiguration.class);
    private PhoenixHiveConfigurationUtil util;
    private final Configuration conf = null;

    private String Quorum = ConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
    private String Port = ConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
    private String Parent = ConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
    private String TableName;
    private String DbName;
    private long BatchSize = ConfigurationUtil.DEFAULT_UPSERT_BATCH_SIZE;

    public PhoenixHiveConfiguration(Configuration conf) {
        //this.conf = conf;
        this.util = new PhoenixHiveConfigurationUtil();
    }

    public PhoenixHiveConfiguration(Table tbl) {
        Map<String, String> mps = tbl.getParameters();
        String quorum =
                mps.get(ConfigurationUtil.ZOOKEEPER_QUORUM) != null ? mps
                        .get(ConfigurationUtil.ZOOKEEPER_QUORUM)
                        : ConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
        String port =
                mps.get(ConfigurationUtil.ZOOKEEPER_PORT) != null ? mps
                        .get(ConfigurationUtil.ZOOKEEPER_PORT)
                        : ConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
        String parent =
                mps.get(ConfigurationUtil.ZOOKEEPER_PARENT) != null ? mps
                        .get(ConfigurationUtil.ZOOKEEPER_PARENT)
                        : ConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
        String pk = mps.get(ConfigurationUtil.PHOENIX_ROWKEYS);
        if (!parent.startsWith("/")) {
            parent = "/" + parent;
        }
        String tablename =
                (mps.get(ConfigurationUtil.TABLE_NAME) != null) ? mps
                        .get(ConfigurationUtil.TABLE_NAME) : tbl.getTableName();

        String mapping = mps.get(ConfigurationUtil.COLUMN_MAPPING);
    }

    public void configure(String server, String tableName, long batchSize) {
        //configure(server, tableName, batchSize, null);
    }

    public void configure(String quorum, String port, String parent, long batchSize,
            String tableName, String dbname, String columns) {
        Quorum = quorum != null ? quorum : ConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
        Port = port != null ? port : ConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
        Parent = parent != null ? parent : ConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
        // BatchSize = batchSize!=null?batchSize:ConfigurationUtil.DEFAULT_UPSERT_BATCH_SIZE;
    }

    static class PhoenixHiveConfigurationUtil {

        public Connection getConnection(final Configuration configuration) throws SQLException {
            Preconditions.checkNotNull(configuration);
            Properties props = new Properties();
            //final Connection conn =
                    //DriverManager.getConnection(QueryUtil.getUrl(configuration.get(SERVER_NAME)),props).unwrap(PhoenixConnection.class);
            //conn.setAutoCommit(false);
            return null;
        }
    }

}