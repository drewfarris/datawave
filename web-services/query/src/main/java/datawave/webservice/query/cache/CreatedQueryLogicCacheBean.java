package datawave.webservice.query.cache;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RunAs;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.limit.QueryHeartbeat;
import datawave.webservice.query.limit.QueryLimiter;
import datawave.webservice.query.runner.RunningQuery;

@Startup
@Singleton
@RunAs("InternalUser")
@PermitAll
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
@LocalBean
public class CreatedQueryLogicCacheBean {

    public static class Triple {

        final String userID;
        final QueryLogic<?> logic;
        final AccumuloClient client;
        final Creation creation;

        public Triple(String userID, QueryLogic<?> logic, AccumuloClient client) {
            this(userID, logic, client, null);
        }

        private Triple(String userID, QueryLogic<?> logic, AccumuloClient client, Creation creation) {
            this.userID = userID;
            this.logic = logic;
            this.client = client;
            this.creation = creation;
        }

        @Override
        public int hashCode() {
            return ((new HashCodeBuilder()).append(userID).append(logic).append(client)).toHashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Triple) {
                Triple other = (Triple) o;
                return Objects.equals(other.userID, userID) && Objects.equals(other.logic, logic) && Objects.equals(other.client, client)
                                && other.creation == creation;
            }

            return false;
        }
    }

    private static final Function<Triple,Pair<QueryLogic<?>,AccumuloClient>> tripToPair = from -> {
        if (null == from) {
            return null;
        }
        return from.creation != null ? from.creation : Pair.of(from.logic, from.client);
    };

    // returns the logic and connection fields in a triple as a pair
    private static final Function<Entry<Pair<String,Long>,Triple>,Entry<String,Pair<QueryLogic<?>,AccumuloClient>>> toPair = from -> {
        if (from == null) {
            return null;
        } else {
            return Maps.immutableEntry(from.getKey().getLeft(), tripToPair.apply(from.getValue()));
        }
    };

    private static final Logger log = Logger.getLogger(CreatedQueryLogicCacheBean.class);

    @Inject
    private AccumuloConnectionFactory connectionFactory;
    private final ConcurrentHashMap<Pair<String,Long>,Triple> cache = new ConcurrentHashMap<>();
    private Clock clock = Clock.systemUTC();

    /**
     * One creation owns its loan until publication transfers ownership to RunningQuery. Temporary-cache cleanup and the creating caller share this return
     * claim, even before construction completes. Keep failed and pending returns reachable until cleanup succeeds.
     */
    public static class Creation extends Pair<QueryLogic<?>,AccumuloClient> {
        private static final long serialVersionUID = 1L;
        private final CreatedQueryLogicCacheBean owner;
        private final String queryId;
        private final QueryLogic<?> logic;
        // Guarded by this; factory and logic callbacks run outside this monitor.
        private AccumuloClient client;
        private RunningQuery query;
        private QueryCache runningCache;
        private QueryLimiter limiter;
        private QueryHeartbeat heartbeat;
        private boolean closed;
        private boolean returnClaimed;
        private boolean returned;
        private boolean published;
        private boolean creatorFinished;

        private Creation(CreatedQueryLogicCacheBean owner, String queryId, QueryLogic<?> logic) {
            this.owner = owner;
            this.queryId = queryId;
            this.logic = logic;
        }

        @Override
        public QueryLogic<?> getLeft() {
            return logic;
        }

        @Override
        public synchronized AccumuloClient getRight() {
            return client;
        }

        @Override
        public AccumuloClient setValue(AccumuloClient value) {
            throw new UnsupportedOperationException("Creation owns its client");
        }

        public synchronized void acquiredClient(AccumuloClient client) {
            this.client = client;
        }

        public synchronized void setHeartbeat(QueryLimiter limiter, QueryHeartbeat heartbeat) {
            this.limiter = limiter;
            this.heartbeat = heartbeat;
        }

        public synchronized void attach(RunningQuery query) throws QueryException {
            this.query = query;
            if (closed) {
                query.markForRemoval();
            }
            checkForCancellation();
        }

        public synchronized void checkForCancellation() throws QueryException {
            if (closed) {
                throw new QueryException(DatawaveErrorCode.QUERY_CANCELED, "Creation was closed before initialization completed");
            }
        }

        public synchronized void publish(QueryCache cache) throws QueryException {
            checkForCancellation();
            query.checkForRemoval();
            cache.put(queryId, query);
            runningCache = cache;
            published = true;
        }

        public synchronized boolean isInitializing() {
            return !closed && !published && !creatorFinished;
        }

        public void close(AccumuloConnectionFactory factory) throws Exception {
            RunningQuery closingQuery;
            QueryCache cache;
            QueryLimiter closingLimiter;
            QueryHeartbeat closingHeartbeat;
            AccumuloClient returningClient = null;
            boolean transferred;
            synchronized (this) {
                closed = true;
                closingQuery = query;
                cache = runningCache;
                closingLimiter = limiter;
                closingHeartbeat = heartbeat;
                transferred = published;
                if (!transferred && client != null && !returnClaimed && !returned) {
                    returnClaimed = true;
                    returningClient = client;
                }
            }
            try {
                if (closingQuery != null) {
                    closingQuery.markForRemoval();
                    if (!transferred) {
                        closingQuery.getMetric().setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
                        closingQuery.terminateResultsThread();
                        closingQuery.setClient(null);
                    }
                }
                if (transferred) {
                    closingQuery.closeConnection(factory);
                } else if (returningClient != null) {
                    factory.returnClient(returningClient);
                    synchronized (this) {
                        returned = true;
                    }
                }
            } catch (Exception e) {
                if (returningClient != null) {
                    synchronized (this) {
                        returnClaimed = false;
                    }
                }
                throw e;
            } finally {
                if (!transferred) {
                    try {
                        logic.close();
                    } catch (Exception e) {
                        log.error("Error closing creation logic for " + queryId, e);
                    }
                }
                if (closingLimiter != null && closingHeartbeat != null) {
                    try {
                        closingLimiter.stopCountingQueryTowardsLimits(queryId, closingHeartbeat);
                    } catch (Exception e) {
                        log.error("Error stopping creation accounting for " + queryId, e);
                    }
                }
                if (transferred && !closingQuery.hasOutstandingClientLease()) {
                    cache.remove(queryId, closingQuery);
                }
                retireIfDone();
            }
        }

        public void finish() {
            synchronized (this) {
                creatorFinished = true;
            }
            retireIfDone();
        }

        private void retireIfDone() {
            synchronized (this) {
                if (creatorFinished && (published || returned || client == null)) {
                    owner.removeCreation(this);
                }
            }
        }
    }

    /**
     * Register ownership before borrowing or construction, so any cleanup path can terminate the same creation.
     *
     * @param queryId
     *            the query identifier
     * @param userId
     *            the creating user
     * @param logic
     *            the creation's logic
     * @return its shared initialization and cleanup owner
     */
    public Creation addForCreation(String queryId, String userId, QueryLogic<?> logic) {
        Creation creation = new Creation(this, queryId, logic);
        if (cache.putIfAbsent(Pair.of(queryId, clock.millis()), new Triple(userId, logic, null, creation)) != null) {
            throw new IllegalStateException("Creation already registered for " + queryId);
        }
        return creation;
    }

    public boolean isInitializing(String queryId) {
        for (Entry<Pair<String,Long>,Triple> entry : cache.entrySet()) {
            if (entry.getKey().getLeft().equals(queryId)) {
                Creation creation = entry.getValue().creation;
                if (creation == null || creation.isInitializing()) {
                    return true;
                }
            }
        }
        return false;
    }

    private void removeCreation(Creation creation) {
        for (Entry<Pair<String,Long>,Triple> entry : cache.entrySet()) {
            if (entry.getValue().creation == creation) {
                cache.remove(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Add the provided QueryLogic to the QueryLogicCache.
     *
     * @param queryId
     *            a query id
     * @param userId
     *            a user id
     * @param logic
     *            the query logic
     * @param client
     *            accumulo client
     * @return true if there was no previous mapping for the given queryId in the cache.
     */
    public boolean add(String queryId, String userId, QueryLogic<?> logic, AccumuloClient client) {
        Triple value = new Triple(userId, logic, client);
        long updateTime = clock.millis();
        return cache.putIfAbsent(Pair.of(queryId, updateTime), value) == null;
    }

    public Pair<QueryLogic<?>,AccumuloClient> poll(String id) {
        Entry<Pair<String,Long>,Triple> entry = get(id);
        return entry == null ? null : tripToPair.apply(entry.getValue());
    }

    public Map<String,Pair<QueryLogic<?>,AccumuloClient>> snapshot() {
        HashMap<Pair<String,Long>,Triple> snapshot = Maps.newHashMap(cache);
        HashMap<String,Pair<QueryLogic<?>,AccumuloClient>> tformMap = Maps.newHashMapWithExpectedSize(cache.size());

        for (Entry<String,Pair<QueryLogic<?>,AccumuloClient>> tform : Iterables.transform(snapshot.entrySet(), toPair)) {
            tformMap.put(tform.getKey(), tform.getValue());
        }
        return tformMap;
    }

    public Map<String,Pair<QueryLogic<?>,AccumuloClient>> entriesOlderThan(final Long now, final Long expiration) {
        Iterable<Entry<Pair<String,Long>,Triple>> iter = Iterables.filter(cache.entrySet(), input -> {
            Long timeInserted = input.getKey().getRight();

            // If this entry was inserted more than the TTL 'time' ago, do not return it
            if ((now - expiration) > timeInserted) {
                return true;
            }

            return false;
        });

        Map<String,Pair<QueryLogic<?>,AccumuloClient>> result = Maps.newHashMapWithExpectedSize(32);
        for (Entry<Pair<String,Long>,Triple> entry : iter) {
            result.put(entry.getKey().getLeft(), tripToPair.apply(entry.getValue()));
        }

        return result;
    }

    public Pair<QueryLogic<?>,AccumuloClient> pollIfOwnedBy(String queryId, String userId) {
        Entry<Pair<String,Long>,Triple> entry = get(queryId);
        if (entry != null) {
            if (userId.equals(entry.getValue().userID)) {
                return tripToPair.apply(entry.getValue());
            } else if (entry.getValue().creation == null) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }

        return null;
    }

    public void clearQueryLogics(long currentTimeMs, long timeToLiveMs) {
        Set<Entry<String,Pair<QueryLogic<?>,AccumuloClient>>> entrySet = entriesOlderThan(currentTimeMs, timeToLiveMs).entrySet();
        clearQueryLogics(entrySet);
    }

    private void clearQueryLogics(Set<Entry<String,Pair<QueryLogic<?>,AccumuloClient>>> entrySet) {
        int count = 0;
        for (Entry<String,Pair<QueryLogic<?>,AccumuloClient>> entry : entrySet) {
            Pair<QueryLogic<?>,AccumuloClient> activePair = poll(entry.getKey());

            if (activePair == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Could not find identified pair needing qlCache eviction. Someone removed him from underneath us");
                }
                continue;
            }

            if (activePair instanceof Creation) {
                try {
                    ((Creation) activePair).close(connectionFactory);
                } catch (Exception e) {
                    log.error("Could not complete creation cleanup for " + entry.getKey(), e);
                }
                continue;
            }

            try {
                activePair.getLeft().close();
            } catch (Exception ex) {
                log.error("Exception caught while closing an uninitialized query logic.", ex);
            }

            try {
                connectionFactory.returnClient(activePair.getRight());
            } catch (Exception ex) {
                log.error("Could not return connection from: " + entry.getKey() + " - " + activePair, ex);
            }

            count++;
        }

        if (count > 0 && log.isDebugEnabled()) {
            log.debug(count + " entries evicted from the query logic cache.");
        }
    }

    @PreDestroy
    public void shutdown() {
        clearQueryLogics(snapshot().entrySet());
    }

    /**
     * Finds and entry in the underlying cache with the given queryId. Returns null if no such element in the map is found. Will only return the
     * arbitrarily-found 'first' entry as we shouldn't have such a collision in the first place
     *
     * @param queryId
     *            the query id
     * @return entry in the underlying cache with the given queryId
     */
    private Entry<Pair<String,Long>,Triple> get(String queryId) {
        for (Pair<String,Long> key : cache.keySet()) {
            if (key.getLeft().equals(queryId)) {
                Triple value = cache.get(key);
                if (value != null && value.creation != null) {
                    // The creating caller may still be initializing or a failed return may need another API cleanup attempt.
                    return Maps.immutableEntry(key, value);
                }
                value = cache.remove(key);
                if (value != null) {
                    return Maps.immutableEntry(key, value);
                }
            }
        }

        return null;
    }

    /**
     * Primarily used for testing
     *
     * @param clock
     *            the clock
     */
    public void setClock(Clock clock) {
        this.clock = clock;
    }
}
