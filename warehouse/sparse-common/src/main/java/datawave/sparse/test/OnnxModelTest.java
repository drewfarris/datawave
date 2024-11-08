package datawave.sparse.test;

import ai.djl.modality.nlp.DefaultVocabulary;
import ai.djl.modality.nlp.Vocabulary;
import ai.djl.modality.nlp.bert.BertFullTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OnnxModelTest {
    public static final String MODEL_NAME = "splade-pp-ed-optimized-1.onnx";
    public static final String VOCAB_NAME = "splade-pp-ed-vocab-1.txt";

    public static final int MAX_SEQ_LEN = 512;

    protected final BertFullTokenizer tokenizer;

    protected final DefaultVocabulary vocabulary;

    protected final OrtEnvironment environment;

    protected final OrtSession session;

    private static final Logger log = LogManager.getLogger(OnnxModelTest.class);

    public OnnxModelTest(String vocabName, String modelName) throws IOException, OrtException {
        this.vocabulary = DefaultVocabulary.builder().addFromTextFile(getVocabPath(vocabName)).optUnknownToken("[UNK]").build();
        this.tokenizer = new BertFullTokenizer(vocabulary, true);
        this.environment = OrtEnvironment.getEnvironment();
        this.session = environment.createSession(getModelPath(modelName).toString(), new OrtSession.SessionOptions());
    }

    public static String getCacheDir() throws IOException {
        File cacheDir = new File("warehouse/sparse-common/target/models");
        if (!cacheDir.exists()) {
            throw new IOException("Could not find model cache dire in " + cacheDir);
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

    protected static long[] convertTokensToIds(List<String> tokens, Vocabulary vocab, int maxLen) {
        int numTokens = Math.min(tokens.size(), maxLen);
        long[] tokenIds = new long[numTokens];
        for (int i = 0; i < numTokens; ++i) {
            tokenIds[i] = vocab.getIndex(tokens.get(i));
        }
        return tokenIds;
    }

    public Map<String,Float> encode(String query, int maxLen) throws OrtException {
        return getTokenWeightMap(query, maxLen);
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
        Map<String,Float> tokenWeightMap = null;
        try (OrtSession.Result results = session.run(inputs)) {
            long[] indexes = (long[]) results.get("output_idx").get().getValue();
            float[] weights = (float[]) results.get("output_weights").get().getValue();
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

    public void run() {

    }

    public static void main(String[] args) throws Exception {
        OnnxModelTest test = new OnnxModelTest(VOCAB_NAME, MODEL_NAME);
        Map<String,Float> result = test.encode("The rain in Spain falls mainly on the the plane", MAX_SEQ_LEN);
        result.forEach((k, v) -> System.out.println(k + " " + v));
    }
}
