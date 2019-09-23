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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

public class BaseSourceConfigTest {
  static class BaseSourceConfigBuilder extends BaseSourceConfig {

    BaseSourceConfigBuilder() {
      super("ref");
    }

    static BaseSourceConfig withIdsAndType(String adId, String adSetId, String accountId, String campaignId,
                                           String objectType) {
      BaseSourceConfig result = new BaseSourceConfigBuilder();
      result.adId = adId;
      result.adSetId = adSetId;
      result.accountId = accountId;
      result.campaignId = campaignId;
      result.objectType = objectType;
      return result;
    }
  }

  @Test
  public void testValidateObjectIdEmptyId() {
    BaseSourceConfig config = BaseSourceConfigBuilder.withIdsAndType("", "", "", "", "Ad");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateObjectId(failureCollector);

    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateObjectId() {
    BaseSourceConfig config = BaseSourceConfigBuilder.withIdsAndType("adId", "", "", "", "Ad");
    MockFailureCollector failureCollector = new MockFailureCollector();
    config.validateObjectId(failureCollector);

    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals("adId", config.getObjectId());
  }
}
