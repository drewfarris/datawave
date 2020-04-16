package datawave.query.transformer;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.lucene.document.Field;

import datawave.ingest.data.RawRecordContainer;
import datawave.marking.MarkingFunctions;
import datawave.query.search.Term;
import datawave.query.table.parser.TermFrequencyKeyValueFactory.TermFrequencyKeyValue;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.result.BaseQueryResponse;

public class TermFrequencyQueryTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,RawRecordContainer> {
    
    private Query query = null;
    private Authorizations auths = null;
    
    public TermFrequencyQueryTransformer(Query query, MarkingFunctions markingFunctions) {
        super(markingFunctions);
        this.query = query;
        this.auths = new Authorizations(StringUtils.split(this.query.getQueryAuthorizations(), ','));
    }
    
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponse response = new EventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            RawRecordContainer result = (RawRecordContainer) o;
            eventList.add(result);
        }
        response.setEvents(eventList);
        response.setReturnedEvents(new Long(eventList.size()));
        return response;
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
        
        Metadata m = new Metadata();
        m.setRow(tfkv.getShardId());
        m.setDataType(tfkv.getShardId());
        m.setInternalId(tfkv.getUit());
        e.setMetadata(m);

        List<Field> fields = ImmutableList.of(
            createField(tfkv, entry, "FIELD_NAME", tfkv.getFieldName()),
            createField(tfkv, entry, "FIELD_VALUE", tfkv.getFieldValue()),
            createField(tfkv, entry, "OFFSET_COUNT", tfkv.getCount()),
            createField(tfkv, entry, "OFFETS", tfkv.getOffsets().toString())
        )
        e.setFields(fields);

        return e;
    }
    
    protected Field createField(TermFrequencyKeyValue tfkv, Entry<Key,Value> e, String name, String value) {
        Field field = new Field();
        field.setMarkings(tfkv.getMarkings());
        field.setName(name);
        field.setValue(value);
        field.setTimestamp(e.getKey().getTimestamp());
        return field;
    }
}
