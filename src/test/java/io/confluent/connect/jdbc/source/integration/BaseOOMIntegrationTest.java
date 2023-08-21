package io.confluent.connect.jdbc.source.integration;

import static junit.framework.TestCase.assertTrue;

import io.confluent.connect.jdbc.source.AccessSourceConnectorConfig;
import io.confluent.connect.jdbc.source.AccessSourceTask;
import io.confluent.connect.jdbc.source.AccessSourceTaskConfig;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for OOM integration tests.
 *
 * <p>These tests expect to be run with -Xmx64M or -Xmx128M for minimum runtime but will behave
 * correctly with any heap size (just with less performance).
 */
public abstract class BaseOOMIntegrationTest {

  private static Logger log = LoggerFactory.getLogger(BaseOOMIntegrationTest.class);

  public static final long MAX_MEMORY = Runtime.getRuntime().maxMemory();

  public static final int BYTES_PER_ROW = 1024;
  // enough rows to take up the whole heap
  public static final long LARGE_QUERY_ROW_COUNT = MAX_MEMORY / BYTES_PER_ROW;

  protected abstract String buildLargeQuery();

  public Map<String, String> props;
  public AccessSourceTask task;

  public void startTask() {
    task = new AccessSourceTask();
    task.start(props);
  }

  @After
  public void stopTask() {
    if (task != null) {
      task.stop();
    }
  }

  @Test
  public void testStreamingReads() throws InterruptedException {
    props.put(AccessSourceTaskConfig.TABLES_CONFIG, "");
    props.put(AccessSourceTaskConfig.TABLES_FETCHED, "true");
    props.put(AccessSourceConnectorConfig.QUERY_CONFIG, buildLargeQuery());
    startTask();
    assertTrue(task.poll().size() > 0);
  }
}
