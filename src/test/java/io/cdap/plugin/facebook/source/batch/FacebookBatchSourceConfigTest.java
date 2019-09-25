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

package io.cdap.plugin.facebook.source.batch;

import com.facebook.ads.sdk.AdsInsights;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

public class FacebookBatchSourceConfigTest {

  static class FacebookBatchSourceConfigBuilder extends FacebookBatchSourceConfig {

    FacebookBatchSourceConfigBuilder() {
      super("ref");
    }

    static FacebookBatchSourceConfig withBreakdown(String breakdown) {
      FacebookBatchSourceConfigBuilder result = new FacebookBatchSourceConfigBuilder();
      result.breakdown = breakdown;
      return result;
    }

    static FacebookBatchSourceConfig withSorting(String sorting, String direction) {
      FacebookBatchSourceConfigBuilder result = new FacebookBatchSourceConfigBuilder();
      result.sorting = sorting;
      result.sortDirection = direction;
      return result;
    }
  }

  @Test
  public void testValidateBreakdown() {
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigBuilder.withBreakdown("age, gender *");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateBreakdowns(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertTrue(config.getBreakdown().getBreakdowns().contains(AdsInsights.EnumBreakdowns.VALUE_AGE));
    Assert.assertTrue(config.getBreakdown().getBreakdowns().contains(AdsInsights.EnumBreakdowns.VALUE_GENDER));
  }

  @Test
  public void testValidateBreakdownInvalid() {
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigBuilder.withBreakdown("age, gender, invalid *");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateBreakdowns(failureCollector);

    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testSorting() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigBuilder.withSorting("field", "descending");
    config.validateSorting(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals("field_descending", config.getSorting());
  }

  @Test
  public void testSortingEmptyField() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigBuilder.withSorting(null, "descending");
    config.validateSorting(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertNull(config.getSorting());
  }

  @Test
  public void testSortingInvalidDirection() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfigBuilder.withSorting("field", "nowhere").validateSorting(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }
}
