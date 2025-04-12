package datawave.annotation.model;

import datawave.data.hash.UID;

import java.util.List;
import java.util.Map;

public class Annotation {
    private UID id;
    private Map<String,String> metadata;
    private List<Segment> segments;
}
