package datawave.ingest.table.aggregator;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.webservice.results.cached.result.CachedresultMessages;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class CardinalityAggregator extends PropogatingCombiner {

    private MarkingFunctions markingFunction = MarkingFunctionsFactory.createMarkingFunctions();


    private static final Logger log = Logger.getLogger(CardinalityAggregator.class);

    /**
     * Flag to determine if we propogate the removals
     */
    protected boolean propogate = true;

    private CardinalityAggregator() { }

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        if (log.isTraceEnabled())
            log.trace("has next ? " + iter.hasNext());

        try {
            if (iter.hasNext()) {
                HyperLogLogPlus combinedHllp = HyperLogLogPlus.Builder.build(iter.next().get());
                while (iter.hasNext()) {
                    combinedHllp.addAll(HyperLogLogPlus.Builder.build(iter.next().get()));
                }
                return new Value(combinedHllp.getBytes());
            }
        }
        catch (IOException | CardinalityMergeException e) {
            throw new RuntimeException("Unexpected error aggregating cardinalities " + e.toString(), e);
        }

        // what to return when the input iterator has no entries?
    }

    public void reset() {
        if (log.isDebugEnabled())
            log.debug("Resetting CardinalityAggregator");
    }

    @Override
    public boolean propogateKey() {
        return true;
    }
}
