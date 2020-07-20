package datawave.ingest.mapreduce.handler.facet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.inject.internal.util.$AsynchronousComputationException;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.webservice.results.cached.result.CachedresultMessages;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FacetAggregatorIterator implements SortedKeyValueIterator<Key, Value> {

    private static final Logger log = Logger.getLogger(FacetAggregatorIterator.class);
    private SortedKeyValueIterator<Key, Value> source;

    private Key topKey;
    private Value topValue;

    private ArrayList<ColumnVisibility> combiningVisibilities = new ArrayList<>();
    private MarkingFunctions markingFunction = MarkingFunctionsFactory.createMarkingFunctions();
    private String visibility;

    public FacetAggregatorIterator() {
        super();
    }

    public FacetAggregatorIterator(FacetAggregatorIterator other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
    }
    @Override
    public void init(SortedKeyValueIterator<Key, Value> sortedKeyValueIterator, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
        this.source = sortedKeyValueIterator.deepCopy(iteratorEnvironment);
        this.visibility = map.get("visibility");
    }

    @Override
    public boolean hasTop() {
        return topKey != null;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        source.seek(range, columnFamilies, inclusive);
        next();
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment iteratorEnvironment) {
        return new FacetAggregatorIterator(this, iteratorEnvironment);
    }

    @Override
    public void next() throws IOException {
        topKey = null;
        topValue = null;

        Key lastKey = null;
        ColumnVisibility minVis = null;
        HyperLogLogPlus combinedHllp = null;

        if (source.hasTop()) {
            Key topKey = source.getTopKey();
            Value topValue = source.getTopValue();
            lastKey = topKey;
            combinedHllp = HyperLogLogPlus.Builder.build(topValue.get());
            if (visibility == null) {
                try {
                    minVis = markingFunction.combine(Collections.singletonList(new ColumnVisibility(topKey.getColumnVisibility())));
                }
                catch (MarkingFunctions.Exception mfe) {
                    throw new IOException(mfe);
                }
            }
            else {
                minVis = new ColumnVisibility(visibility);
            }
            source.next();
        }

        while (source.hasTop()) {
            Key topKey =  source.getTopKey();
            Value topValue = source.getTopValue();
            // TODO? collect and merge up at end.
            if (topKey.equals(lastKey,PartialKey.ROW_COLFAM_COLQUAL)) {
                HyperLogLogPlus newHllp = HyperLogLogPlus.Builder.build(topValue.get());
                try {
                    combinedHllp.addAll(newHllp);
                    if (visibility == null) {
                        combiningVisibilities.clear();
                        combiningVisibilities.add(minVis);
                        combiningVisibilities.add(new ColumnVisibility(topKey.getColumnVisibility()));
                        minVis = markingFunction.combine(combiningVisibilities);
                    }
                    else if (minVis == null) {
                        minVis = new ColumnVisibility(visibility);
                    }
                }
                catch (MarkingFunctions.Exception | CardinalityMergeException e) {
                    throw new IOException(e);
                }
                source.next();
            }
            else {
                break;
            }
        }

        if (lastKey != null) {
            topKey = new Key(lastKey.getRow(), lastKey.getColumnFamily(), lastKey.getColumnQualifier(), minVis, lastKey.getTimestamp());
            topValue = new Value(combinedHllp.getBytes());
        }
    }
}
