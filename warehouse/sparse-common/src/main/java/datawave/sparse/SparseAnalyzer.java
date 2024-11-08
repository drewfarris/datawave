package datawave.sparse;

import ai.djl.modality.nlp.DefaultVocabulary;
import ai.djl.modality.nlp.bert.BertFullTokenizer;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import org.apache.lucene.analysis.Analyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.nio.file.Path;

public class SparseAnalyzer extends Analyzer {

    static final Logger log = LoggerFactory.getLogger(SparseTokenizer.class);
    public static final int MAX_SEQ_LEN = 512;

    public static final float MIN_SPARSE_WEIGHT = 0.1f;

    protected final BertFullTokenizer tokenizer;

    protected final DefaultVocabulary vocabulary;

    protected final OrtEnvironment environment;

    protected final OrtSession session;

    protected int maxSequenceLength = MAX_SEQ_LEN;

    protected float minSparseWeight = MIN_SPARSE_WEIGHT;

    public SparseAnalyzer(String vocabName, String modelName) throws IOException {
        try {
            this.vocabulary = DefaultVocabulary.builder().addFromTextFile(getVocabPath(vocabName)).optUnknownToken("[UNK]").build();
            this.tokenizer = new BertFullTokenizer(vocabulary, true);
            this.environment = OrtEnvironment.getEnvironment();
            this.session = environment.createSession(getModelPath(modelName).toString(), new OrtSession.SessionOptions());
        }
        catch (IOException | OrtException e) {
            throw new IOException("Error creating onnxruntime session", e);
        }

    }

    public int getMaxSequenceLength() {
        return maxSequenceLength;
    }

    public void setMaxSequenceLength(int maxSequenceLength) {
        this.maxSequenceLength = maxSequenceLength;
    }

    public float getMinSparseWeight() {
        return minSparseWeight;
    }

    public void setMinSparseWeight(float minSparseWeight) {
        this.minSparseWeight = minSparseWeight;
    }

    public static String getCacheDir() throws IOException {
        File cacheDir = new File("target/models");
        if (!cacheDir.exists()) {
            throw new IOException("Could not find model cache dir in " + cacheDir);
        }
        return cacheDir.getPath();
    }

    public Path getVocabPath(String vocabName) throws IOException {
        File vocabFile = new File(getCacheDir(), vocabName);
        if (!vocabFile.exists()) {
            throw new IOException("Could not find vocabulary in " + vocabFile);
        } else {
            log.info("Vocabulary found in {}", vocabName);
        }
        return vocabFile.toPath();
    }

    public Path getModelPath(String modelName) throws IOException {
        File modelFile = new File(getCacheDir(), modelName);
        if (!modelFile.exists()) {
            throw new IOException("Could not find model in " + modelFile);
        } else {
            log.info("Model found in {}", modelFile);
        }
        return modelFile.toPath();
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final SparseTokenizer src = new SparseTokenizer(tokenizer, vocabulary, environment, session, maxSequenceLength, minSparseWeight);
        return new TokenStreamComponents(src);
    }
}
