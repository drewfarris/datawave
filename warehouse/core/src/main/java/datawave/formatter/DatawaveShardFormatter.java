package datawave.formatter;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.ingest.protobuf.TermWeight;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import java.util.List;
import java.util.Map;

public class DatawaveShardFormatter extends DatawaveFormatter {
    
    public static final byte[] TF = "tf".getBytes();
    public static final String EMPTY_STRING = "";
    
    private final StringBuilder sb = new StringBuilder();
    private final Text holder = new Text();
    
    public static final String hex_null = "\\x00";
    
    @Override
    protected String formatEntry(Map.Entry<Key,Value> entry) {
        if (entry == null) {
            return EMPTY_STRING;
        }
        
        // reset the size.
        sb.setLength(0);
        
        Text colf = entry.getKey().getColumnFamily();
        if (colf.getBytes() != null && colf.getBytes().length > 0 && WritableComparator.compareBytes(TF, 0, TF.length, colf.getBytes(), 0, TF.length) == 0) {
            return formatTermFrequencyEntry(entry);
        } else {
            // TODO: fi? d column?
            return formatEventEntry(entry);
        }
    }
    
    public static final boolean notEmptyList(List<?> list) {
        return list != null && list.size() > 0;
    }
    
    protected String formatTermFrequencyEntry(Map.Entry<Key,Value> entry) {
        appendKey(sb, entry.getKey());
        
        // append term positions
        final Value v = entry.getValue();
        if (v != null && v.getSize() > 0) {
            sb.append("\t{");
        }
        
        TermWeight.Info termWeightInfo;
        try {
            termWeightInfo = TermWeight.Info.parseFrom(v.get());
        } catch (InvalidProtocolBufferException e) {
            sb.append("Could not deserialize ProfocolBuffer");
            return sb.toString();
        }
        
        sb.append("Size=").append(termWeightInfo.getTermOffsetCount()).append(", ZeroOffsetMatch=").append(termWeightInfo.getZeroOffsetMatch());
        
        // TODO: cleanup, eliminate duplicate code.
        
        if (notEmptyList(termWeightInfo.getTermOffsetList())) {
            sb.append(", Offsets=[");
            for (Integer i : termWeightInfo.getTermOffsetList()) {
                sb.append(i).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]");
        } else {
            sb.append(", Offsets=[]");
        }
        
        if (notEmptyList(termWeightInfo.getPrevSkipsList())) {
            sb.append(", PrevSkips=[");
            for (Integer i : termWeightInfo.getPrevSkipsList()) {
                sb.append(i).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]");
        } else {
            sb.append(", PrevSkips=[]");
        }
        
        if (notEmptyList(termWeightInfo.getScoreList())) {
            sb.append(", Scores=[");
            for (Integer i : termWeightInfo.getScoreList()) {
                sb.append(i).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]");
        } else {
            sb.append(", Scores=[]");
        }
        
        return sb.toString();
    }
    
    protected String formatEventEntry(Map.Entry<Key,Value> entry) {
        appendKey(sb, entry.getKey());
        formatValue(sb, entry.getValue()).append("\n");
        return sb.toString();
    }
    
    protected StringBuilder appendKey(StringBuilder sb, Key k) {
        // append row
        k.getRow(holder);
        appendByteArray(sb, holder.getBytes(), 0, holder.getLength()).append(" ");
        
        // append column family
        k.getColumnFamily(holder);
        appendByteArray(sb, holder.getBytes(), 0, holder.getLength()).append(" ");
        
        // append column qualifier
        k.getColumnQualifier(holder);
        appendByteArray(sb, holder.getBytes(), 0, holder.getLength()).append(" ");
        
        // append visibility expression
        sb.append(new ColumnVisibility(k.getColumnVisibility()));
        
        // append timestamp
        if (this.doTimestamps)
            sb.append(" ").append(k.getTimestamp());
        
        return sb;
    }
    
    protected StringBuilder appendByteArray(StringBuilder sb, byte[] data, int start, int len) {
        int last = 0;
        int curr = 0;
        while (curr < len) {
            if (data[curr] == 0) {
                // FIXME: this is likely not handling multibyte characters properly.
                sb.append(new String(data, last, curr - last)).append(hex_null);
                curr++;
                last = curr;
            }
            curr++;
        }
        
        sb.append(new String(data, last, curr - last));
        
        return sb;
    }
}
