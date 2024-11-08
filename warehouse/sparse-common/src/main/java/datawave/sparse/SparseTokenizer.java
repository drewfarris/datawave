package datawave.sparse;

import ai.djl.modality.nlp.DefaultVocabulary;
import ai.djl.modality.nlp.Vocabulary;
import ai.djl.modality.nlp.bert.BertFullTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SparseTokenizer extends Tokenizer {

    private final CharTermAttribute termAtt;

    private final SparseWeightAttribute sparseWeightAtt;

    protected final BertFullTokenizer tokenizer;

    protected final DefaultVocabulary vocabulary;

    protected final OrtEnvironment environment;

    protected final OrtSession session;

    protected final int maxSeqLen;

    protected final float minSparseWeight;

    protected Iterator<Map.Entry<String, Float>> tokenWeightIterator = null;

    protected SparseTokenizer(BertFullTokenizer tokenizer, DefaultVocabulary vocabulary, OrtEnvironment environment, OrtSession session, int maxSeqLan, float minSparseWeight) {
        super();
        this.tokenizer = tokenizer;
        this.vocabulary = vocabulary;
        this.environment = environment;
        this.session = session;
        this.maxSeqLen = maxSeqLan;
        this.minSparseWeight = minSparseWeight;

        termAtt = addAttribute(CharTermAttribute.class);
        sparseWeightAtt = addAttribute(SparseWeightAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (tokenWeightIterator == null) {
            try {
                String targetString = IOUtils.toString(input);
                Map<String, Float> tokenWeightMap = getTokenWeightMap(targetString, maxSeqLen);
                tokenWeightIterator = tokenWeightMap.entrySet().iterator();
            }
            catch (IOException | OrtException e) {
                throw new IOException("Could not generate token weight map", e);
            }
        }

        while (tokenWeightIterator.hasNext()) {
            Map.Entry<String, Float> e = tokenWeightIterator.next();
            if (e.getValue() >= minSparseWeight) {
                char[] buffer = e.getKey().toCharArray();
                termAtt.copyBuffer(buffer, 0, buffer.length);
                sparseWeightAtt.setSparseWeight(e.getValue());
                return true;
            }
        }

        return false;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        this.tokenWeightIterator = null;
    }

    protected static long[] convertTokensToIds(List<String> tokens, Vocabulary vocab, int maxLen) {
        int numTokens = Math.min(tokens.size(), maxLen);
        long[] tokenIds = new long[numTokens];
        for (int i = 0; i < numTokens; ++i) {
            tokenIds[i] = vocab.getIndex(tokens.get(i));
        }
        return tokenIds;
    }

    public Map<String,Float> getTokenWeightMap(String query, int maxLen) throws OrtException {
        List<String> queryTokens = new ArrayList<>();
        queryTokens.add("[CLS]");
        queryTokens.addAll(tokenizer.tokenize(query));
        queryTokens.add("[SEP]");

        Map<String,OnnxTensor> inputs = new HashMap<>();
        long[] queryTokenIds = convertTokensToIds(queryTokens, vocabulary, maxLen);
        long[][] inputTokenIds = new long[1][queryTokenIds.length];

        inputTokenIds[0] = queryTokenIds;
        long[][] attentionMask = new long[1][queryTokenIds.length];
        long[][] tokenTypeIds = new long[1][queryTokenIds.length];
        // initialize attention mask with all 1s
        Arrays.fill(attentionMask[0], 1);
        inputs.put("input_ids", OnnxTensor.createTensor(environment, inputTokenIds));
        inputs.put("token_type_ids", OnnxTensor.createTensor(environment, tokenTypeIds));
        inputs.put("attention_mask", OnnxTensor.createTensor(environment, attentionMask));
        Map<String,Float> tokenWeightMap;
        try (OrtSession.Result results = session.run(inputs)) {
            Optional<OnnxValue> outputIndexes = results.get("output_idx");
            long[] indexes =  outputIndexes.isPresent() ? ((long[]) outputIndexes.get().getValue()) : new long[0];

            Optional<OnnxValue> outputWeights = results.get("output_weights");
            float[] weights = outputWeights.isPresent() ? ((float[]) outputWeights.get().getValue()) : new float[0];

            tokenWeightMap = getTokenWeightMap(indexes, weights, vocabulary);
        }
        return tokenWeightMap;
    }

    static protected Map<String,Float> getTokenWeightMap(long[] indexes, float[] computedWeights, DefaultVocabulary vocab) {
        Map<String,Float> tokenWeightMap = new LinkedHashMap<>();

        for (int i = 0; i < indexes.length; i++) {
            if (indexes[i] == 101 || indexes[i] == 102 || indexes[i] == 0) {
                continue;
            }
            tokenWeightMap.put(vocab.getToken(indexes[i]), computedWeights[i]);
        }
        return tokenWeightMap;
    }
}
