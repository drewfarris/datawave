package datawave.query.tables.ssdeep;

import datawave.query.discovery.DiscoveredThing;

public class DiscoveredSSDeep {
    public final ScoredSSDeepPair scoredSSDeepPair;
    public final DiscoveredThing discoveredThing;
    
    public DiscoveredSSDeep(ScoredSSDeepPair scoredSSDeepPair, DiscoveredThing discoveredThing) {
        this.scoredSSDeepPair = scoredSSDeepPair;
        this.discoveredThing = discoveredThing;
    }
    
    public ScoredSSDeepPair getScoredSSDeepPair() {
        return scoredSSDeepPair;
    }
    
    public DiscoveredThing getDiscoveredThing() {
        return discoveredThing;
    }
}
