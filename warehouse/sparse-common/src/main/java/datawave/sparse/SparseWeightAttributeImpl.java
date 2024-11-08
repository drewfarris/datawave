package datawave.sparse;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

public class SparseWeightAttributeImpl extends AttributeImpl implements SparseWeightAttribute {
    private float sparseWeight = 1.0f;

    @Override
    public void setSparseWeight(float sparseWeight) {
        this.sparseWeight = sparseWeight;
    }

    public float getSparseWeight() {
        return sparseWeight;
    }

    @Override
    public void clear() {
        sparseWeight = 1.0f;
    }

    @Override
    public void copyTo(AttributeImpl target) {
        ((SparseWeightAttribute) target).setSparseWeight(sparseWeight);
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
        reflector.reflect(SparseWeightAttribute.class, "sparseWeight", sparseWeight);
    }
}
