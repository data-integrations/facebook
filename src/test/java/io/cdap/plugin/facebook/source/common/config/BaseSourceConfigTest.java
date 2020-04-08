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

package io.cdap.plugin.facebook.source.common.config;

import com.facebook.ads.sdk.AdsInsights;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.facebook.source.BaseFacebookValidationTest;
import io.cdap.plugin.facebook.source.batch.FacebookBatchSourceConfig;
import org.junit.Assert;
import org.junit.Test;

public class BaseSourceConfigTest extends BaseFacebookValidationTest {
  @Test
  public void testValidateObjectIdEmptyId() {
    BaseSourceConfig config = FacebookBatchSourceConfig.builder()
      .setAdId("").setAdSetId("").setAccountId("").setCampaignId("").setObjectType("Ad").build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateObjectId(failureCollector);

    assertSingleFieldValidationFailed(failureCollector, FacebookBatchSourceConfig.PROPERTY_AD_ID);
  }

  @Test
  public void testValidateObjectId() {
    BaseSourceConfig config = FacebookBatchSourceConfig.builder()
      .setAdId("adId").setAdSetId("").setAccountId("").setCampaignId("").setObjectType("Ad").build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateObjectId(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals("adId", config.getObjectId());
  }

  @Test
  public void testValidateFields() {
    BaseSourceConfig config = FacebookBatchSourceConfig.builder()
      .setFields("date_start,impressions").build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFields(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFieldsInvalidField() {
    BaseSourceConfig config = FacebookBatchSourceConfig.builder()
      .setFields("date_start,impressions,invalid").build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFields(failureCollector);

    assertSingleFieldValidationFailed(failureCollector, FacebookBatchSourceConfig.PROPERTY_FIELDS);
  }


  @Test
  public void testValidateFiltering() {
    BaseSourceConfig config = FacebookBatchSourceConfig.builder()
      .setFiltering("value1:CONTAIN(adset.name)" + BaseSourceConfig.FILTERING_DELIMITER + "value2:EQUAL(field2)")
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFiltering(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(2, config.getFilters().size());
  }

  @Test
  public void testValidateFilteringEmptyOrNull() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    ((BaseSourceConfig) FacebookBatchSourceConfig.builder().setFiltering("").build())
      .validateFiltering(failureCollector);
    ((BaseSourceConfig) FacebookBatchSourceConfig.builder().setFiltering(null).build())
      .validateFiltering(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFilteringInvalidOp() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    ((BaseSourceConfig) FacebookBatchSourceConfig.builder().setFiltering("value:INVALID(field)").build())
      .validateFiltering(failureCollector);
    assertSingleFieldValidationFailed(failureCollector, FacebookBatchSourceConfig.PROPERTY_FILTERING);
  }

  @Test
  public void testValidateFilteringInvalidFormat() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    ((BaseSourceConfig) FacebookBatchSourceConfig.builder().setFiltering("valueEQUALS(field").build())
      .validateFiltering(failureCollector);

    assertSingleFieldValidationFailed(failureCollector, FacebookBatchSourceConfig.PROPERTY_FILTERING);
  }

  @Test
  public void testValidateBreakdown() {
    BaseSourceConfig config = FacebookBatchSourceConfig.builder()
      .setBreakdown("age, gender *").build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateBreakdowns(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertTrue(config.getBreakdown().getBreakdowns().contains(AdsInsights.EnumBreakdowns.VALUE_AGE));
    Assert.assertTrue(config.getBreakdown().getBreakdowns().contains(AdsInsights.EnumBreakdowns.VALUE_GENDER));
  }

  @Test
  public void testValidateBreakdownInvalid() {
    BaseSourceConfig config = FacebookBatchSourceConfig.builder()
      .setBreakdown("age, gender, invalid *").build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateBreakdowns(failureCollector);

    assertSingleFieldValidationFailed(failureCollector, FacebookBatchSourceConfig.PROPERTY_BREAKDOWN);
  }
}
