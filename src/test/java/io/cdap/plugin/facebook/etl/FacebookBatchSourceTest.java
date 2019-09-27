/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.facebook.etl;

import com.facebook.ads.sdk.APIContext;
import com.facebook.ads.sdk.APIException;
import com.facebook.ads.sdk.AdAccount;
import com.facebook.ads.sdk.AdSet;
import com.facebook.ads.sdk.Campaign;
import com.google.common.base.Strings;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class FacebookBatchSourceTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static String accessToken = null;
  private static AdAccount account;
  private static List<Campaign> campaigns = new ArrayList<>();
  private static List<AdSet> adSets = new ArrayList<>();

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // initialize fb api
    accessToken = System.getProperty("fb.access.token");
    if (Strings.isNullOrEmpty(accessToken)) {
      throw new IllegalArgumentException("fb.access.token system property must not be empty.");
    }

    String adAccount = System.getProperty("fb.ad.account");
    if (Strings.isNullOrEmpty(adAccount)) {
      throw new IllegalArgumentException("fb.ad.account system property must not be empty.");
    }

    APIContext context = new APIContext(accessToken);
    account = new AdAccount(adAccount, context);


    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      FacebookBatchSource.class);
  }

  @AfterClass
  public static void tearDownTestClass() {
    adSets.forEach(adSet -> {
      try {
        adSet.delete().execute();
      } catch (APIException ex) {
        //ignore
      }
    });
    campaigns.forEach(campaign -> {
      try {
        campaign.delete().execute();
      } catch (APIException ex) {
        //ignore
      }
    });
  }

  private static Campaign createCampaign(String name) throws APIException {
    Campaign result = account.createCampaign()
      .setName(name)
      .setObjective(Campaign.EnumObjective.VALUE_LINK_CLICKS)
      .setSpendCap(10000L)
      .setStatus(Campaign.EnumStatus.VALUE_PAUSED)
      .execute();

    campaigns.add(result);
    return result;
  }

  private static AdSet createAdSet(String name, Campaign campaign) throws APIException {
    int year = Calendar.getInstance().get(Calendar.YEAR);
    AdSet result = account.createAdSet()
      .setCampaignId(campaign.getId())
      .setName(name)
      .setStatus(AdSet.EnumStatus.VALUE_PAUSED)
      .setBillingEvent(AdSet.EnumBillingEvent.VALUE_IMPRESSIONS)
      .setDailyBudget(1000L)
      .setBidAmount(100L)
      .setStartTime(String.format("%s-01-01T00:00:00+0000", year))
      .setEndTime(String.format("%s-01-01T00:00:00+0000", year + 1))
      .setOptimizationGoal("REACH")
      .setTargeting("{\"geo_locations\":{\"countries\":[\"US\"]},\"publisher_platforms\":[\"facebook\"]}")
      .execute();

    adSets.add(result);

    return result;
  }

  @Test
  public void testBatchSource() throws Exception {
    String campaignName = "CDAP-test-campaign-" + UUID.randomUUID().toString();
    String adSet1Name = "CDAP-ad-set-1-" + UUID.randomUUID().toString();
    String adSet2Name = "CDAP-ad-set-2-" + UUID.randomUUID().toString();

    Campaign campaign = createCampaign(campaignName);

    createAdSet(adSet1Name, campaign);
    createAdSet(adSet2Name, campaign);

    ETLStage source = new ETLStage("source", new ETLPlugin(
      FacebookBatchSource.NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("referenceName", "ref")
        .put("accessToken", accessToken)
        .put("objectType", "Campaign")
        .put("campaignId", campaign.getId())
        .put("level", "adset")
        .put("sorting", "adset_name")
        .put("sortDirection", "ascending")
        .put("fields", "impressions,campaign_id,adset_id,adset_name")
        .put("datePreset", "lifetime")
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

    Assert.assertEquals(2, outputRecords.size());

    // sorting is ascending by ad set name, so records ordering must be okay
    Assert.assertEquals(adSet1Name, outputRecords.get(0).get("adset_name"));
    Assert.assertEquals(adSet2Name, outputRecords.get(1).get("adset_name"));
  }
}
