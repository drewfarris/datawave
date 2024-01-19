package datawave.query.tables.ssdeep;

import datawave.ingest.mapreduce.handler.ssdeep.NGramTuple;
import datawave.ingest.mapreduce.handler.ssdeep.SSDeepHash;
import datawave.query.discovery.DiscoveredThing;
import datawave.query.tables.chained.strategy.FullChainStrategy;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

public class FullSSDeepDiscoveryChainStrategy extends FullChainStrategy<Entry<SSDeepHash, NGramTuple>, DiscoveredThing> {
    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<Entry<SSDeepHash, NGramTuple>> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");
        StringBuilder b = new StringBuilder();
        Set<String> ssdeepSeen = new HashSet<>();
        while (initialQueryResults.hasNext()) {
            Map.Entry<SSDeepHash, NGramTuple> result = initialQueryResults.next();
            SSDeepHash key = result.getKey();
            String ssdeep = key.toString();
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
        return q;
    }

    @Override
    public Iterator<DiscoveredThing> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths, Iterator<Entry<SSDeepHash, NGramTuple>> initialQueryResults, QueryLogic<DiscoveredThing> latterQueryLogic) throws Exception {
        return super.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);
    }
}
