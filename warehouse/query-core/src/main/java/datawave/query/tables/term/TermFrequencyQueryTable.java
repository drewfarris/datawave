package datawave.query.tables.term;

import java.util.Map.Entry;

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.query.QueryParameters;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;

public class TermFrequencyQueryTable extends BaseQueryLogic<Entry<Key, Value>> {

    protected static final Logger log = ThreadConfigurableLogger.getLogger(TermFrequencyQueryTable.class);

    public TermFrequencyQueryTable() {
        super();
        log.debug("Creating TermFrequencyQueryTable: " + System.identityHashCode(this));
    }

    public TermFrequencyQueryTable(BaseQueryLogic<Entry<Key, Value>> other) {
        super(other);
        log.debug("Creating TermFrequencyQueryTable: " + System.identityHashCode(this));
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new TermFrequencyQueryTable(this);
    }

    @Override
    public Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        return ImmutableSet.of("termfrequency.field.name","termfrequency.match.term","termfrequency.find.term", QueryParameters.CONTENT_VIEW_ALL);
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new TermFrequencyQueryTransformer(settings, markingFunctions);
    }

    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings,
            Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        // TODO Auto-generated method stub

    }
}