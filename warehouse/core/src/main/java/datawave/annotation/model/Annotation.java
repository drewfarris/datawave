package datawave.annotation.model;

import java.util.List;
import java.util.Map;

import datawave.data.hash.UID;

public class Annotation {
    private String eventShard;
    private String eventDataType;
    private UID eventUID;
    private UID annotationId;

    // what is this and how do we store it?
    private Map<String,String> metadata;

    private List<Segment> segments;
}
