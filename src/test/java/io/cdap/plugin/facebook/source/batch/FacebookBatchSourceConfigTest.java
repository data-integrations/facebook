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

    static FacebookBatchSourceConfig withFiltering(String filtering) {
      FacebookBatchSourceConfigBuilder result = new FacebookBatchSourceConfigBuilder();
      result.filtering = filtering;
      return result;
    }

    static FacebookBatchSourceConfig withBreakdown(String breakdown) {
      FacebookBatchSourceConfigBuilder result = new FacebookBatchSourceConfigBuilder();
      result.breakdown = breakdown;
      return result;
    }

    static FacebookBatchSourceConfig withFields(String fields) {
      FacebookBatchSourceConfigBuilder result = new FacebookBatchSourceConfigBuilder();
      result.fields = fields;
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
  public void testValidateFields() {
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigBuilder.withFields("date_start,impressions");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFields(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFieldsInvalidField() {
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigBuilder.withFields("date_start,impressions,invalid");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFields(failureCollector);

    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
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
  public void testValidateFiltering() {
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigBuilder.withFiltering(
      "value:EQUAL(field)" + FacebookBatchSourceConfigBuilder.FILTERING_DELIMITER + "value2:EQUAL(field2)");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateFiltering(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(2, config.getFilters().size());
  }

  @Test
  public void testValidateFilteringEmptyOrNull() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfigBuilder.withFiltering("").validateFiltering(failureCollector);
    FacebookBatchSourceConfigBuilder.withFiltering(null).validateFiltering(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFilteringInvalidOp() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfigBuilder.withFiltering("value:INVALID(field)").validateFiltering(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateFilteringInvalidFormat() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfigBuilder.withFiltering("valueEQUALS(field").validateFiltering(failureCollector);
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
