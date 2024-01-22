package datawave.query.tables.ssdeep;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
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
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FullSSDeepDiscoveryChainStrategy extends FullChainStrategy<ScoredSSDeepPair, DiscoveredSSDeep> {

    Multimap<String, ScoredSSDeepPair> scoredMatches;

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<ScoredSSDeepPair> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");

        // track the scored matches we've seen while traversing the initial query results.
        // this has to be case insensitive because the CHECKSUM_SSDEEP index entries are most likely downcased.
        scoredMatches = TreeMultimap.create(
                String.CASE_INSENSITIVE_ORDER,
                ScoredSSDeepPair.NATURAL_ORDER
        );

        // extract the matched ssdeeps from the query results and generate the discovery query.
        StringBuilder b = new StringBuilder();
        Set<String> ssdeepSeen = new HashSet<>();
        while (initialQueryResults.hasNext()) {
            ScoredSSDeepPair result = initialQueryResults.next();
            SSDeepHash matchingHash = result.getMatchingHash();
            scoredMatches.put(matchingHash.toString(), result);
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
        return q;
    }

    @Override
    public Iterator<DiscoveredSSDeep> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths, Iterator<ScoredSSDeepPair> initialQueryResults, QueryLogic<DiscoveredSSDeep> latterQueryLogic) throws Exception {
        final Iterator<DiscoveredSSDeep> it = super.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);

        // Create a defensive copy of the score map because stream evaluation may be delayed.
        final Multimap<String, ScoredSSDeepPair> localScoredMatches = TreeMultimap.create(
                String.CASE_INSENSITIVE_ORDER,
                ScoredSSDeepPair.NATURAL_ORDER);
        localScoredMatches.putAll(scoredMatches);

        // For each of the discovered SSDeep hashes returned by the discovery logic, enrish them with the original
        // query and scores.
        final Stream<DiscoveredSSDeep> stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
                .flatMap(discoveredSSDeep -> {
                    DiscoveredThing thing = discoveredSSDeep.getDiscoveredThing();
                    String term = thing.getTerm();
                    // This will return zero to many new DiscoveredSSDeep entries for each query that the matching ssdeep hash appeared in.
                    return localScoredMatches.get(term).stream().map(scoredPair -> new DiscoveredSSDeep(scoredPair, discoveredSSDeep.getDiscoveredThing()));
                });

        return stream.iterator();
    }
}
