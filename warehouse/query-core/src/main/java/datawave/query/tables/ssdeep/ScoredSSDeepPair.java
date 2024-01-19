package datawave.query.tables.ssdeep;

import datawave.ingest.mapreduce.handler.ssdeep.SSDeepHash;

import java.util.Objects;

/**
 * Captures a scored query hash and matching hash pair
 */
public class ScoredSSDeepPair {
    private final SSDeepHash queryHash;
    private final SSDeepHash matchingHash;
    int weightedScore;

    public ScoredSSDeepPair(SSDeepHash queryHash, SSDeepHash matchingHash, int weightedScore) {
        this.queryHash = queryHash;
        this.matchingHash = matchingHash;
        this.weightedScore = weightedScore;
    }

    public SSDeepHash getQueryHash() {
        return queryHash;
    }

    public SSDeepHash getMatchingHash() {
        return matchingHash;
    }

    public int getWeightedScore() {
        return weightedScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScoredSSDeepPair that = (ScoredSSDeepPair) o;

        if (weightedScore != that.weightedScore) return false;
        if (!Objects.equals(queryHash, that.queryHash)) return false;
        return Objects.equals(matchingHash, that.matchingHash);
    }

    @Override
    public int hashCode() {
        int result = queryHash != null ? queryHash.hashCode() : 0;
        result = 31 * result + (matchingHash != null ? matchingHash.hashCode() : 0);
        result = 31 * result + weightedScore;
        return result;
    }
}
