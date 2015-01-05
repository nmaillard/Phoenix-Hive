package org.apache.phoenix.hive;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.phoenix.query.KeyRange;

public class PhoenixInputSplit extends FileSplit {
    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);
    private KeyRange keyRange;
    private Path path;

    public PhoenixInputSplit() {
        super((Path) null, 0, 0, (String[]) null);
    }

    public PhoenixInputSplit(KeyRange keyRange) {
        Preconditions.checkNotNull(keyRange);
        this.keyRange = keyRange;
    }

    public PhoenixInputSplit(KeyRange keyRange, Path path) {
        Preconditions.checkNotNull(keyRange);
        Preconditions.checkNotNull(path);
        LOG.debug("path: " + path);

        this.keyRange = keyRange;
        this.path = path;
    }

    public void readFields(DataInput input) throws IOException {
        this.path = new Path(Text.readString(input));
        this.keyRange = new KeyRange();
        this.keyRange.readFields(input);
    }

    public void write(DataOutput output) throws IOException {
        Preconditions.checkNotNull(this.keyRange);
        Text.writeString(output, path.toString());
        this.keyRange.write(output);
    }

    public long getLength() {
        return 0L;
    }

    public String[] getLocations() {
        return new String[0];
    }

    public KeyRange getKeyRange() {
        return this.keyRange;
    }

    @Override
    public Path getPath() {
        return this.path;
    }

    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = 31 * result + (this.keyRange == null ? 0 : this.keyRange.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof PhoenixInputSplit)) return false;
        PhoenixInputSplit other = (PhoenixInputSplit) obj;
        if (this.keyRange == null) {
            if (other.keyRange != null) return false;
        } else if (!this.keyRange.equals(other.keyRange)) return false;
        return true;
    }
}