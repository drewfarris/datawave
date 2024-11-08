package datawave.sparse;

import org.apache.lucene.util.Attribute;

public interface SparseWeightAttribute extends Attribute {
    public void setSparseWeight(float sparseWeight);
    public float getSparseWeight();
}
