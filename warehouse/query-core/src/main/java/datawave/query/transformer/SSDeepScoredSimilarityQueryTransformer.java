package datawave.query.transformer;

import datawave.marking.MarkingFunctions;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.query.tables.ssdeep.ScoredSSDeepPair;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SSDeepScoredSimilarityQueryTransformer extends BaseQueryLogicTransformer<ScoredSSDeepPair,ScoredSSDeepPair> {

    private static final Logger log = Logger.getLogger(SSDeepScoredSimilarityQueryTransformer.class);

    protected final Authorizations auths;

    protected final ResponseObjectFactory responseObjectFactory;

    public SSDeepScoredSimilarityQueryTransformer(Query query, SSDeepSimilarityQueryConfiguration config, MarkingFunctions markingFunctions,
                                                  ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
        this.responseObjectFactory = responseObjectFactory;
    }

    @Override
    public ScoredSSDeepPair transform(ScoredSSDeepPair input) throws EmptyObjectException {
        return input; /* no-op */
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        return generateResponseFromScores(resultList);
    }

    public BaseQueryResponse generateResponseFromScores(List<Object> resultList) {
        // package the scoredTuples into an event query response
        final EventQueryResponseBase eventResponse = responseObjectFactory.getEventQueryResponse();
        final List<EventBase> events = new ArrayList<>();

        int rank = 1;
        for (Object o : resultList) {
            ScoredSSDeepPair pair = (ScoredSSDeepPair) o;

            final EventBase event = responseObjectFactory.getEvent();
            final List<FieldBase> fields = new ArrayList<>();

            FieldBase f = responseObjectFactory.getField();
            f.setName("MATCHING_SSDEEP");
            f.setValue(pair.getMatchingHash().toString());
            fields.add(f);

            f = responseObjectFactory.getField();
            f.setName("QUERY_SSDEEP");
            f.setValue(pair.getQueryHash().toString());
            fields.add(f);

            f = responseObjectFactory.getField();
            f.setName("MATCH_SCORE");
            // TODO: this really should be the overlap score, the number of ssdeep ngrams that
            //   the two hashes have in common, is this relevant any longer?
            f.setValue(String.valueOf(pair.getWeightedScore()));
            fields.add(f);

            f = responseObjectFactory.getField();
            f.setName("WEIGHTED_SCORE");
            f.setValue(String.valueOf(pair.getWeightedScore()));
            fields.add(f);

            event.setFields(fields);
            events.add(event);

            log.info("    " + rank + ". " + pair);
            rank++;
        }

        eventResponse.setEvents(events);

        return eventResponse;
    }
}
