package datawave.sparse;

import datawave.sparse.util.ResourceReader;
import datawave.sparse.util.SimpleUnitTestUtil;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparseAnalyzerTest {
    static final Logger logger = LoggerFactory.getLogger(SparseAnalyzerTest.class);
    static final String OUTPUT_DIR = "target/output/SparseAnalyzerTest";

    public static final String MODEL_NAME = "splade-pp-ed-optimized-1.onnx";
    public static final String VOCAB_NAME = "splade-pp-ed-vocab-1.txt";

    protected String name;
    protected String actual;
    protected String expected;

    public static Collection<Object[]> data() {
        return SimpleUnitTestUtil.getInputFilesForTest(SparseAnalyzerTest.class);
    }

    @ParameterizedTest
    @MethodSource({"data"})
    void testSparseAnalyzer(String resource, String name) throws IOException {
        ResourceReader rr = new ResourceReader();
        InputStream doc = rr.getResourceAsStream(resource);
        Reader input = new InputStreamReader(doc, Charset.defaultCharset());
        this.expected = SimpleUnitTestUtil.getExpectedOutputContentByResourcePath(resource);
        this.name = name;
        tokenize(input);
    }

    public void tokenize(Reader input) throws IOException {
        StringBuilder b;
        try (SparseAnalyzer analyzer = new SparseAnalyzer(VOCAB_NAME, MODEL_NAME); TokenStream ts = analyzer.tokenStream("test", input)) {
            ts.reset();

            StringBuilder a = new StringBuilder();
            b = new StringBuilder();

            CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
            SparseWeightAttribute weight = ts.getAttribute(SparseWeightAttribute.class);

            while (ts.incrementToken()) {
                a.setLength(0);
                a.append(term).append(' ').append(weight.getSparseWeight()).append("\n");
                b.append(a);
            }
        }
        actual = b.toString();
        SimpleUnitTestUtil.writeOutput(OUTPUT_DIR, actual, logger, name);
        assertEquals(expected, actual);

    }
}
