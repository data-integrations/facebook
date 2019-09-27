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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

public class FacebookBatchSourceConfigTest {

  static class FacebookBatchSourceConfigMock extends FacebookBatchSourceConfig {

    FacebookBatchSourceConfigMock() {
      super("ref");
    }

    static FacebookBatchSourceConfig withSorting(String sorting, String direction) {
      FacebookBatchSourceConfigMock result = new FacebookBatchSourceConfigMock();
      result.sorting = sorting;
      result.sortDirection = direction;
      return result;
    }
  }

  @Test
  public void testSorting() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigMock.withSorting("field", "descending");
    config.validateSorting(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals("field_descending", config.getSorting());
  }

  @Test
  public void testSortingEmptyField() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfig config = FacebookBatchSourceConfigMock.withSorting(null, "descending");
    config.validateSorting(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertNull(config.getSorting());
  }

  @Test
  public void testSortingInvalidDirection() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfigMock.withSorting("field", "nowhere").validateSorting(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }
}
