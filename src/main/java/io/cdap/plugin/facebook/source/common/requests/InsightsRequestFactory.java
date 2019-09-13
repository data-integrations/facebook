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

package io.cdap.plugin.facebook.source.common.requests;

import com.facebook.ads.sdk.APIContext;
import com.facebook.ads.sdk.Ad;
import com.facebook.ads.sdk.AdAccount;
import com.facebook.ads.sdk.AdSet;
import com.facebook.ads.sdk.Campaign;
import io.cdap.plugin.facebook.source.common.BaseSourceConfig;
import io.cdap.plugin.facebook.source.common.ObjectType;

/**
 * Creates request based on source configuration.
 */
public class InsightsRequestFactory {
  private static InsightsRequest createRequest(ObjectType objectType, String objectId, String accessToken) {
    APIContext context = new APIContext(accessToken).enableDebug(true);
    switch (objectType) {
      case Campaign:
        return new InsightsRequestWrapper(new Campaign(objectId, context).getInsights());
      case Ad:
        return new InsightsRequestWrapper(new Ad(objectId, context).getInsights());
      case AdSet:
        return new InsightsRequestWrapper(new AdSet(objectId, context).getInsights());
      case Account:
        return new InsightsRequestWrapper(new AdAccount(objectId, context).getInsights());
      default:
        throw new IllegalArgumentException("Unsupported object");
    }
  }

  /**
   * Creates insights request.
   */
  public static InsightsRequest createRequest(BaseSourceConfig config) {
    return createRequest(config.getObjectType(), config.getObjectId(), config.getAccessToken());
  }
}
