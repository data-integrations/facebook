package io.cdap.plugin.facebook.etl;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.facebook.source.batch.FacebookBatchSource;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Ignore
public class FacebookSourceETLTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      FacebookBatchSource.class);
  }

  @Test
  public void getPipelineResults() throws Exception {
    ETLStage source = new ETLStage("source", new ETLPlugin(
      FacebookBatchSource.NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("referenceName", "ref")
        .put("accessToken", "token")
        .put("objectType", "Campaign")
        .put("objectId", "120330000173671207")
        .put("level", "adset")
        .put("fields", "impressions,campaign_id,adset_id")
        .build(),
      null)
    );
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin("outputSink"));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("HttpBatch_");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset("outputSink");
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    System.out.println();
  }
}
