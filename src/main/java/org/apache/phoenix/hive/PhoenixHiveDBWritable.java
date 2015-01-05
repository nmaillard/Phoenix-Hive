package org.apache.phoenix.hive;

import com.google.common.base.Preconditions;

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

public class PhoenixHiveDBWritable implements Writable, DBWritable {
    private static final Log LOG = LogFactory.getLog(PhoenixHiveDBWritable.class);

    private final List<Object> values = new ArrayList();
    private PDataType[] PDataTypes = null;
    ResultSet rs;

    public PhoenixHiveDBWritable() {
    }

    public PhoenixHiveDBWritable(PDataType[] categories) {
        this.PDataTypes = categories;
    }

    public void readFields(ResultSet rs) throws SQLException {
        Preconditions.checkNotNull(rs);
        this.rs = rs;
    }

    public List<Object> getValues() {
        return this.values;
    }

    public Object get(String name) {
        try {
            return this.rs.getObject(name);
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

    public void add(Object value) {
        this.values.add(value);
    }

    public void clear() {
        this.values.clear();
    }

    public void write(PreparedStatement statement) throws SQLException {
        for (int i = 0; i < this.values.size(); i++) {
            Object o = this.values.get(i);
            try {
                if (o != null) {
                    LOG.debug(" value " + o.toString() + " type "
                            + this.PDataTypes[i].getSqlTypeName() + " int value "
                            + this.PDataTypes[i].getSqlType());
                    statement.setObject(i + 1, PDataType
                            .fromTypeId(this.PDataTypes[i].getSqlType()).toObject(o.toString()));
                } else {
                    LOG.debug(" value NULL  type " + this.PDataTypes[i].getSqlTypeName()
                            + " int value " + this.PDataTypes[i].getSqlType());
                    statement.setNull(i + 1, this.PDataTypes[i].getSqlType());
                }
            } catch (RuntimeException re) {
                throw new RuntimeException(String.format(
                    "Unable to process column %s, innerMessage=%s",
                    new Object[] { re.getMessage() }), re);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
    }

    public void write(DataOutput arg0) throws IOException {
    }
}