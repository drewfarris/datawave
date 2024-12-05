package datawave.ingest.trec;

import com.google.common.collect.Multimap;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.json.config.helper.JsonIngestHelper;
import datawave.ingest.json.mr.input.JsonRecordReader;
import datawave.ingest.json.util.JsonObjectFlattener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Date;

public class TestTrecJsonConfig {

    @Test
    public void testGetEventFields() throws Exception {
        Configuration conf = initConfig();
        JsonIngestHelper ingestHelper = init(conf);

        try (JsonRecordReader reader = initReader(conf)) {
            reader.setInputDate(System.currentTimeMillis());

            Assert.assertTrue(reader.nextKeyValue());
            RawRecordContainer event = reader.getEvent();

            Multimap<String, NormalizedContentInterface> fieldMap = ingestHelper.getEventFields(event);
        }
    }

    protected JsonIngestHelper init(Configuration conf) throws Exception {

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        JsonIngestHelper ingestHelper = new JsonIngestHelper();
        ingestHelper.setup(conf);

        return ingestHelper;
    }

    public Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/trec-ingest-config.xml"));

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        return conf;
    }

    protected JsonRecordReader initReader(Configuration conf) throws Exception {

        TaskAttemptContext ctx = null;
        InputSplit split = null;
        File dataFile = null;

        URL data = TestTrecJsonConfig.class.getResource("/input/my.json");
        Assert.assertNotNull(data);

        dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        split = new FileSplit(p, 0, dataFile.length(), null);
        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        JsonRecordReader reader = new JsonRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }
}
