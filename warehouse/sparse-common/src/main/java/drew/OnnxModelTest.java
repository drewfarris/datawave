package drew;

import ai.djl.modality.nlp.DefaultVocabulary;
import ai.djl.modality.nlp.Vocabulary;
import ai.djl.modality.nlp.bert.BertFullTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OnnxModelTest {
    static private final String MODEL_URL = "https://rgw.cs.uwaterloo.ca/pyserini/data/splade-pp-ed-optimized.onnx";

    static private final String VOCAB_URL = "https://rgw.cs.uwaterloo.ca/pyserini/data/wordpiece-vocab.txt";

    public static final String MODEL_NAME = "splade-pp-ed-optimized.onnx";
    public static final String VOCAB_NAME = "splade-pp-ed-vocab.txt";

    static private final String CACHE_DIR = Paths.get(System.getProperty("user.home"), "/.cache/onnx/encoders")
            .toString();

    public static final int MAX_SEQ_LEN = 512;

    protected final BertFullTokenizer tokenizer;

    protected final DefaultVocabulary vocabulary;

    protected final OrtEnvironment environment;

    protected final OrtSession session;

    private static final Logger log = LogManager.getLogger(OnnxModelTest.class);

    public OnnxModelTest(String vocabName, String vocabUrl, String modelName, String modelUrl) throws IOException, OrtException {
        this.vocabulary = DefaultVocabulary.builder()
                .addFromTextFile(getVocabPath(vocabName, vocabUrl))
                .optUnknownToken("[UNK]")
                .build();
        this.tokenizer = new BertFullTokenizer(vocabulary, true);
        this.environment = OrtEnvironment.getEnvironment();
        this.session = environment.createSession(getModelPath(modelName, modelUrl).toString(),
                new OrtSession.SessionOptions());
    }

    public Path getVocabPath(String vocabName, String vocabUrl) throws IOException {
        File vocabFile = new File(getCacheDir(), vocabName);
        if (!vocabFile.exists()) {
            log.info("Downloading vocabulary {} to {}", vocabUrl, vocabFile);
            URI vocabUri = URI.create(vocabUrl);
            FileUtils.copyURLToFile(vocabUri.toURL(), vocabFile);
            log.info("Vocabulary download to {} complete", vocabFile);
        }
        else {
            log.info("Vocabulary exists in {}, skipping download", vocabName);
        }
        return vocabFile.toPath();
    }

    public Path getModelPath(String modelName, String modelUrl) throws IOException {
        File modelFile = new File(getCacheDir(), modelName);
        if (!modelFile.exists()) {
            log.info("Downloading model {} to {}", modelUrl, modelFile);
            URI modelUri = URI.create(modelUrl);
            FileUtils.copyURLToFile(modelUri.toURL(), modelFile);
            log.info("Model download to {} complete", modelFile);
        }
        else {
            log.info("Model exists in {}, skipping download", modelFile);
        }
        return modelFile.toPath();
    }

    public static String getCacheDir() {
        File cacheDir = new File(CACHE_DIR);
        if (!cacheDir.exists()) {
            cacheDir.mkdir();
        }
        return cacheDir.getPath();
    }

    protected static long[] convertTokensToIds(List<String> tokens, Vocabulary vocab, int maxLen) {
        int numTokens = Math.min(tokens.size(), maxLen);
        long[] tokenIds = new long[numTokens];
        for (int i = 0; i < numTokens; ++i) {
            tokenIds[i] = vocab.getIndex(tokens.get(i));
        }
        return tokenIds;
    }

    public Map<String, Float> encode(String query, int maxLen) throws OrtException {
        return getTokenWeightMap(query, maxLen);
    }

    public Map<String, Float> getTokenWeightMap(String query, int maxLen) throws OrtException {
        List<String> queryTokens = new ArrayList<>();
        queryTokens.add("[CLS]");
        queryTokens.addAll(tokenizer.tokenize(query));
        queryTokens.add("[SEP]");

        Map<String, OnnxTensor> inputs = new HashMap<>();
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
        Map<String, Float> tokenWeightMap = null;
        try (OrtSession.Result results = session.run(inputs)) {
            long[] indexes = (long[]) results.get("output_idx").get().getValue();
            float[] weights = (float[]) results.get("output_weights").get().getValue();
            tokenWeightMap = getTokenWeightMap(indexes, weights, vocabulary);
        }
        return tokenWeightMap;
    }

    static protected Map<String, Float> getTokenWeightMap(long[] indexes, float[] computedWeights,
                                                          DefaultVocabulary vocab) {
        Map<String, Float> tokenWeightMap = new LinkedHashMap<>();

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
        OnnxModelTest test = new OnnxModelTest(VOCAB_NAME, VOCAB_URL, MODEL_NAME, MODEL_URL);
        Map<String, Float> result = test.encode("The rain in Spain falls mainly on the the plane", MAX_SEQ_LEN);
        result.forEach((k, v) -> System.out.println(k + " " + v));
    }
}
