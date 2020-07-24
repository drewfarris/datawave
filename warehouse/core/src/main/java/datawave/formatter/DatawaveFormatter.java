package datawave.formatter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.hadoop.io.Text;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public abstract class DatawaveFormatter implements Formatter {
    
    public static final String HEX_NULL = "\\x00";
    
    protected Iterator<Entry<Key,Value>> iter = null;
    protected boolean doTimestamps = false;
    
    private final Text _holder = new Text();
    
    @Override
    public void initialize(Iterable<Map.Entry<Key,Value>> scanner, FormatterConfig config) {
        iter = scanner.iterator();
        doTimestamps = config.willPrintTimestamps();
    }
    
    @Override
    public void remove() {
        checkState();
    }
    
    @Override
    public boolean hasNext() {
        checkState();
        
        return false;
    }
    
    @Override
    public String next() {
        checkState();
        return formatEntry(this.iter.next());
    }
    
    /**
     * Define how to transform a Key-Value pair into a String for display
     * 
     * @param entry
     *            The Key-Value pair to format
     * @return A String reprsentation of the Key-Value pair
     */
    protected abstract String formatEntry(Entry<Key,Value> entry);
    
    /**
     * Check that the formatter is initialized properly
     * 
     * @throws IllegalStateException
     *             if the formatter is in an unexpected state
     */
    protected void checkState() {
        if (iter == null) {
            throw new IllegalStateException("Formatter was not intialized");
        }
    }
    
    protected StringBuilder formatEntry(StringBuilder sb, Entry<Key,Value> entry) {
        formatKey(sb, entry.getKey());
        formatValue(sb, entry.getValue());
        return sb;
    }
    
    protected StringBuilder formatKey(StringBuilder sb, Key k) {
        k.getRow(_holder);
        appendBytes(sb, _holder.getBytes(), 0, _holder.getLength()).append(" ");
        
        k.getColumnFamily(_holder);
        appendBytes(sb, _holder.getBytes(), 0, _holder.getLength()).append(":");
        
        k.getColumnQualifier(_holder);
        appendBytes(sb, _holder.getBytes(), 0, _holder.getLength()).append(" ");
        
        sb.append(new ColumnVisibility(k.getColumnVisibility()));
        
        if (this.doTimestamps) {
            sb.append(" ").append(k.getTimestamp());
        }
        
        return sb;
    }
    
    protected StringBuilder formatValue(StringBuilder sb, Value v) {
        if (v != null && v.getSize() > 0) {
            sb.append("\t");
            appendValue(sb, v);
        }
        return sb;
    }
    
    protected StringBuilder appendText(StringBuilder sb, Text t) {
        return appendBytes(sb, t.getBytes(), 0, t.getLength());
    }
    
    protected StringBuilder appendValue(StringBuilder sb, Value value) {
        return appendBytes(sb, value.get(), 0, value.get().length);
    }
    
    protected StringBuilder appendBytes(StringBuilder sb, byte[] ba, int offset, int len) {
        for (int i = offset; i < len; i++) {
            int c = 0xff & ba[offset + i];
            if (c == '\\') {
                sb.append("\\\\");
            } else if (0 == c) {
                sb.append(HEX_NULL);
            } else {
                sb.append((char) c);
            }
        }
        return sb;
    }
}
