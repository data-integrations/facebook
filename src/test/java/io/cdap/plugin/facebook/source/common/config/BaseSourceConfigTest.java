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
import org.junit.Assert;
import org.junit.Test;

public class BaseSourceConfigTest {
  static class BaseSourceConfigMock extends BaseSourceConfig {

    BaseSourceConfigMock() {
      super("ref");
    }

    static BaseSourceConfig withIdsAndType(String adId, String adSetId, String accountId, String campaignId,
                                           String objectType) {
      BaseSourceConfig result = new BaseSourceConfigMock();
      result.adId = adId;
      result.adSetId = adSetId;
      result.accountId = accountId;
      result.campaignId = campaignId;
      result.objectType = objectType;
      return result;
    }

    static BaseSourceConfig withFiltering(String filtering) {
      BaseSourceConfigMock result = new BaseSourceConfigMock();
      result.filtering = filtering;
      return result;
    }

    static BaseSourceConfig withFields(String fields) {
      BaseSourceConfigMock result = new BaseSourceConfigMock();
      result.fields = fields;
      return result;
    }

    static BaseSourceConfig withBreakdown(String breakdown) {
      BaseSourceConfigMock result = new BaseSourceConfigMock();
      result.breakdown = breakdown;
      return result;
    }
  }

  @Test
  public void testValidateObjectIdEmptyId() {
    BaseSourceConfig config = BaseSourceConfigMock.withIdsAndType("", "", "", "", "Ad");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateObjectId(failureCollector);

    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateObjectId() {
    BaseSourceConfig config = BaseSourceConfigMock.withIdsAndType("adId", "", "", "", "Ad");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateObjectId(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals("adId", config.getObjectId());
  }

  @Test
  public void testValidateFields() {
    BaseSourceConfig config = BaseSourceConfigMock.withFields("date_start,impressions");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFields(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFieldsInvalidField() {
    BaseSourceConfig config = BaseSourceConfigMock.withFields("date_start,impressions,invalid");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFields(failureCollector);

    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }


  @Test
  public void testValidateFiltering() {
    BaseSourceConfig config = BaseSourceConfigMock.withFiltering(
      "value:EQUAL(field)" + BaseSourceConfigMock.FILTERING_DELIMITER + "value2:EQUAL(field2)");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFiltering(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(2, config.getFilters().size());
  }

  @Test
  public void testValidateFilteringEmptyOrNull() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    BaseSourceConfigMock.withFiltering("").validateFiltering(failureCollector);
    BaseSourceConfigMock.withFiltering(null).validateFiltering(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFilteringInvalidOp() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    BaseSourceConfigMock.withFiltering("value:INVALID(field)").validateFiltering(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateFilteringInvalidFormat() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    BaseSourceConfigMock.withFiltering("valueEQUALS(field").validateFiltering(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBreakdown() {
    BaseSourceConfig config = BaseSourceConfigMock.withBreakdown("age, gender *");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateBreakdowns(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertTrue(config.getBreakdown().getBreakdowns().contains(AdsInsights.EnumBreakdowns.VALUE_AGE));
    Assert.assertTrue(config.getBreakdown().getBreakdowns().contains(AdsInsights.EnumBreakdowns.VALUE_GENDER));
  }

  @Test
  public void testValidateBreakdownInvalid() {
    BaseSourceConfig config = BaseSourceConfigMock.withBreakdown("age, gender, invalid *");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateBreakdowns(failureCollector);

    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }
}
