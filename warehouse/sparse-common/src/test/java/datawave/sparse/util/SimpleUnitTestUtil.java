package datawave.sparse.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuppressWarnings("rawtypes")
public class SimpleUnitTestUtil {

    public static final String INPUT_FILE_SUFFIX = ".in";
    public static final String EXPECTED_FILE_SUFFIX = "-expected.txt";
    public static final String ACTUAL_FILE_SUFFIX = "-actual.txt";

    public static Collection<Object[]> getInputFilesForTest(Class testClassName) {
        ResourceReader rr = new ResourceReader();
        List<String> rs = rr.findResourcesFor(testClassName, INPUT_FILE_SUFFIX);
        List<Object[]> al = new ArrayList<>();
        for (String r : rs) {
            int pos = r.lastIndexOf("/");
            String name = r.substring(pos + 1).replace(INPUT_FILE_SUFFIX, "");
            String[] s = {r, name};
            al.add(s);
        }
        return al;
    }

    public static String getExpectedOutputContentByResourcePath(String resource) throws IOException {
        File f = new File(resource);
        File p = f.getParentFile();
        String fname = f.getName();
        String expectedFile = fname.replace(INPUT_FILE_SUFFIX, EXPECTED_FILE_SUFFIX);
        String expectedPath = new File(p, expectedFile).toString();
        ResourceReader rr = new ResourceReader();
        InputStream doc = rr.getResourceAsStream(expectedPath);
        return IOUtils.toString(doc, Charset.defaultCharset());
    }

    public static void writeOutput(String outputDir, String outputContent, Logger logger, String filenamePrefix) {
        File f = new File(outputDir);
        if (f.mkdirs()) {
            logger.debug("Created new output directory {}", f);
        }
        if (!f.isDirectory()) {
            throw new IllegalStateException("Unable to create output directory " + f);
        }
        File output = new File(f, filenamePrefix + ACTUAL_FILE_SUFFIX);
        try {
            Writer fw = Files.newBufferedWriter(output.toPath(), StandardCharsets.UTF_8);
            PrintWriter pw = new PrintWriter(fw);
            pw.write(outputContent);
            pw.close();

        } catch (IOException ex) {
            throw new IllegalStateException("IOException writing output file " + output, ex);
        }
    }
}
