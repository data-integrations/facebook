/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.plugin.facebook.source.BaseFacebookValidationTest;
import org.junit.Assert;
import org.junit.Test;

public class FacebookBatchSourceConfigTest extends BaseFacebookValidationTest {

  @Test
  public void testSorting() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfig config = FacebookBatchSourceConfig.builder()
      .setSorting("field").setSortDirection("descending").build();
    config.validateSorting(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals("field_descending", config.getSorting());
  }

  @Test
  public void testSortingEmptyField() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfig config = FacebookBatchSourceConfig.builder()
      .setSorting(null).setSortDirection("descending").build();
    config.validateSorting(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertNull(config.getSorting());
  }

  @Test
  public void testSortingInvalidDirection() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    FacebookBatchSourceConfig config = FacebookBatchSourceConfig.builder()
      .setSorting("field").setSortDirection("nowhere").build();
    config.validateSorting(failureCollector);

    assertSingleFieldValidationFailed(failureCollector, FacebookBatchSourceConfig.PROPERTY_SORT_DIRECTION);
  }
}
