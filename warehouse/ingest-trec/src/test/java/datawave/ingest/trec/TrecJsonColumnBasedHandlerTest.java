package datawave.ingest.trec;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.json.config.helper.JsonDataTypeHelper;
import datawave.ingest.json.config.helper.JsonIngestHelper;
import datawave.ingest.json.mr.handler.ColumnBasedHandlerTestUtil;
import datawave.ingest.json.mr.handler.ContentJsonColumnBasedHandler;
import datawave.ingest.json.mr.input.JsonRecordReader;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.util.TableName;

@SuppressWarnings("rawtypes")
public class TrecJsonColumnBasedHandlerTest {

    private Configuration conf;
    private static final Logger log = Logger.getLogger(TrecJsonColumnBasedHandlerTest.class);
    private static final Enumeration rootAppenders = Logger.getRootLogger().getAllAppenders();

    @BeforeClass
    public static void setupBeforeClass() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", "UTF8");
    }

    @AfterClass
    public static void tearDownAfterClass() {
        Logger.getRootLogger().removeAllAppenders();
        while (rootAppenders.hasMoreElements()) {
            Appender appender = (Appender) rootAppenders.nextElement();
            Logger.getRootLogger().addAppender(appender);
        }
    }

    private JsonRecordReader getJsonRecordReader(String file) throws IOException, URISyntaxException {
        InputSplit split = ColumnBasedHandlerTestUtil.getSplit(file);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        log.debug(TypeRegistry.getContents());
        JsonRecordReader reader = new JsonRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }

    private static void enableLogging() {
        Logger.getRootLogger().removeAllAppenders();
        ConsoleAppender ca = new ConsoleAppender();
        ca.setLayout(new PatternLayout("%p [%c{1}] %m%n"));
        Logger.getRootLogger().addAppender(ca);
        log.setLevel(Level.TRACE);
        Logger.getLogger(ColumnBasedHandlerTestUtil.class).setLevel(Level.TRACE);
        Logger.getLogger(ContentIndexingColumnBasedHandler.class).setLevel(Level.TRACE);
        Logger.getLogger(ContentBaseIngestHelper.class).setLevel(Level.TRACE);
    }

    private static void disableLogging() {
        log.setLevel(Level.OFF);
        Logger.getLogger(ColumnBasedHandlerTestUtil.class).setLevel(Level.OFF);
        Logger.getLogger(ContentIndexingColumnBasedHandler.class).setLevel(Level.OFF);
        Logger.getLogger(ContentBaseIngestHelper.class).setLevel(Level.OFF);
    }

    @Before
    public void setup() {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.setInt(ShardedDataTypeHandler.NUM_SHARDS, 1);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
    }

    @Test
    public void testTrecJsonContentHandlers() throws Exception {
        enableLogging();

        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/trec-ingest-config.xml"));
        TypeRegistry.getInstance(conf);

        JsonDataTypeHelper helper = new JsonDataTypeHelper();
        helper.setup(conf);

        JsonIngestHelper ingestHelper = new JsonIngestHelper();
        ingestHelper.setup(conf);

        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        ContentJsonColumnBasedHandler<Text> jsonHandler = new ContentJsonColumnBasedHandler<>();
        jsonHandler.setup(context);

        try (JsonRecordReader reader = getJsonRecordReader("/input/msmarco-sample.jsonl")) {
            reader.setInputDate(System.currentTimeMillis());

            Assert.assertTrue("First Record did not read properly?", reader.nextKeyValue());
            RawRecordContainer event = reader.getEvent();
            Assert.assertNotNull("Event 1 was null.", event);
            Assert.assertTrue("Event 1 has parsing errors", event.getErrors().isEmpty());

            ColumnBasedHandlerTestUtil.processEvent(jsonHandler, null, event, 353, 176, 1, 0, false);
        }
    }
}
