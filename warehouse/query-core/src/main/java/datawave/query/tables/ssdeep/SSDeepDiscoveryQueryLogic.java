package datawave.query.tables.ssdeep;

import datawave.ingest.mapreduce.handler.ssdeep.NGramTuple;
import datawave.ingest.mapreduce.handler.ssdeep.SSDeepHash;
import datawave.query.discovery.DiscoveredThing;
import datawave.query.tables.chained.ChainedQueryTable;
import datawave.query.tables.chained.strategy.ChainStrategy;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogicTransformer;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Set;

public class SSDeepDiscoveryQueryLogic extends ChainedQueryTable<Entry<Key, Value>, DiscoveredThing> {

    private static final Logger log = Logger.getLogger(SSDeepDiscoveryQueryLogic.class);

    private Query similarityQuery = null;
    private Query discoveryQuery = null;

    //TODO: perform some generic tricks??
    private ChainStrategy<Map.Entry<SSDeepHash, NGramTuple>, DiscoveredThing> tranasformedChainStrategy;

    public SSDeepDiscoveryQueryLogic() { super(); }

    @SuppressWarnings("CopyConstructorMissesField")
    public SSDeepDiscoveryQueryLogic(SSDeepDiscoveryQueryLogic other) {
        super(other);
    }

    @Override
    public void close() {
        super.close();
    }

    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        super.initialize(client, settings, auths);
        this.similarityQuery = settings;
        this.discoveryQuery = settings.duplicate(settings.getQueryName() + "_discovery_query");

        log.debug("Initial settings parameters: " + settings.getParameters().toString());
        GenericQueryConfiguration config = this.logic1.initialize(client, settings, auths);
        return config;
    }

    public void setupQuery(GenericQueryConfiguration config) throws Exception {
        if (null == this.getTransformedChainStrategy()) {
            final String error = "No transformed ChainStrategy provided for SSDeepDiscoveryQueryLogic!";
            log.error(error);
            throw new RuntimeException(error);
        }

        log.info("Setting up ssdeep query using config");
        this.logic1.setupQuery(config);

        //final Iterator<Entry<Key,Value>> iter1 = this.logic1.iterator();
        final TransformIterator<Entry<Key,Value>, Entry<SSDeepHash, NGramTuple>> transformIterator = this.logic1.getTransformIterator(similarityQuery);

        log.info("Running chained discovery query");
        this.iterator = this.getTransformedChainStrategy().runChainedQuery(config.getClient(), this.discoveryQuery, config.getAuthorizations(), transformIterator, this.logic2);
    }

    public ChainStrategy<Map.Entry<SSDeepHash, NGramTuple>, DiscoveredThing> getTransformedChainStrategy() {
        return this.tranasformedChainStrategy;
    }

    public void setTransformedChainStrategy(ChainStrategy<Map.Entry<SSDeepHash, NGramTuple>, DiscoveredThing> tranasformedChainStrategy) {
        this.tranasformedChainStrategy = tranasformedChainStrategy;
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return this.logic2.getTransformer(settings);
    }

    @Override
    public SSDeepDiscoveryQueryLogic clone() throws CloneNotSupportedException {
        return new SSDeepDiscoveryQueryLogic(this);
    }

    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    } 


}
