package datawave.query.tables.ssdeep;

import datawave.ingest.mapreduce.handler.ssdeep.SSDeepHash;
import datawave.query.tables.chained.strategy.FullChainStrategy;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class FullSSDeepEventChainStrategy extends FullChainStrategy<ScoredSSDeepPair, Map.Entry<Key, Value>> {
    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<ScoredSSDeepPair> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");
        StringBuilder b = new StringBuilder();
        Set<String> ssdeepSeen = new HashSet<>();

        //TODO: rewrite as stream
        while (initialQueryResults.hasNext()) {
            ScoredSSDeepPair result = initialQueryResults.next();
            SSDeepHash matchingHash = result.getMatchingHash();
            String ssdeep = matchingHash.toString();
            if (ssdeepSeen.contains(ssdeep)) {
                continue;
            }
            log.debug("Added new ssdeep " + ssdeep);
            ssdeepSeen.add(ssdeep);
            if (b.length() > 0) {
                b.append(" OR ");
            }
            b.append("CHECKSUM_SSDEEP:\"").append(ssdeep).append("\"");
        }



        Query q = new QueryImpl(); // TODO, need to use a factory? don't hardcode this.
        q.setQuery(b.toString());
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE); // TODO: choose something reasonable.
        q.setQueryAuthorizations(initialQuery.getQueryAuthorizations());
        q.setUserDN(initialQuery.getUserDN());
        // TODO: set up a reasonable start and end date.
        return q;
    }
}
