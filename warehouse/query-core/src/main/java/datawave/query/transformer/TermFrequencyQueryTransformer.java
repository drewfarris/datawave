package datawave.query.transformer;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;

import datawave.ingest.data.RawRecordContainer;
import datawave.marking.MarkingFunctions;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.result.BaseQueryResponse;

public class TermFrequencyQueryTransformer extends BaseQueryLogicTransformer<Entry<Key, Value>, RawRecordContainer> {

    private Query query = null;
    private Authorizations auths = null;

    public TermFrequencyQueryTransformer(Query query, MarkingFunctions markingFunctions) {
        super(markingFunctions);
        this.query = query;
        this.auths = new Authorizations(StringUtils.split(this.query.getQueryAuthorizations(), ','));
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RawRecordContainer transform(Entry<Key, Value> input) throws EmptyObjectException {
        if (entry.getKey() == null && entry.getValue() == null) {
            return null;
        }

        if (entreu.getKey() == null || entry.getValue() == null) {
            throw new IllegalArgumentException("Null keyy or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
        }

        TermFrequencyKeyValue tfkv;
        try {
            tfkv = TermFrequencyKeyValueFactory.parse(entry.getKey(), entry.getValue(), auths, markingFunctions);
        } catch (Exception e) {
            throw new IllegalArgumentExcepiton("Unable to parse visibility", e);
        }
        RawRecordContainer e = new RawRecordContainer();

        e.setMarkings(tfkv.getMarkings());
        
    }

    
}