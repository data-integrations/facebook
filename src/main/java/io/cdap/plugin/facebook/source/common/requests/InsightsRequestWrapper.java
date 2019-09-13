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

package io.cdap.plugin.facebook.source.common.requests;

import com.facebook.ads.sdk.APIException;
import com.facebook.ads.sdk.APINodeList;
import com.facebook.ads.sdk.APIRequest;
import com.facebook.ads.sdk.Ad;
import com.facebook.ads.sdk.AdAccount;
import com.facebook.ads.sdk.AdSet;
import com.facebook.ads.sdk.AdsInsights;
import com.facebook.ads.sdk.Campaign;

import java.util.List;

/**
 * Wraps one of the following requests: {@link Campaign.APIRequestGetInsights}, {@link Ad.APIRequestGetInsights},
 * {@link AdSet.APIRequestGetInsights}, {@link AdAccount.APIRequestGetInsights}.
 */
public class InsightsRequestWrapper implements InsightsRequest {
  private APIRequest<AdsInsights> request;

  InsightsRequestWrapper(APIRequest<AdsInsights> request) {
    this.request = request;
  }

  @Override
  public void requestField(String fieldName) {
    request.requestField(fieldName);
  }

  @Override
  public void setParam(String paramName, Object value) {
    request.setParam(paramName, value);
  }

  @Override
  public void setBreakdowns(List<AdsInsights.EnumBreakdowns> breakdowns) {
    setParam("breakdowns", breakdowns);
  }

  @Override
  public void setActionBreakdowns(List<AdsInsights.EnumActionBreakdowns> breakdowns) {
    setParam("action_breakdowns", breakdowns);
  }

  @Override
  public APINodeList<AdsInsights> execute() throws APIException {
    // every 'APINodeList<AdsInsights> execute()' is the same in requests, but not belongs to common interface
    // that forces us to do this ugly 'ifs' to not to repeat fields and other params setup for each of instance.
    if (request instanceof Campaign.APIRequestGetInsights) {
      return ((Campaign.APIRequestGetInsights) request).execute();
    } else if (request instanceof Ad.APIRequestGetInsights) {
      return ((Ad.APIRequestGetInsights) request).execute();
    } else if (request instanceof AdSet.APIRequestGetInsights) {
      return ((AdSet.APIRequestGetInsights) request).execute();
    } else if (request instanceof AdAccount.APIRequestGetInsights) {
      return ((AdAccount.APIRequestGetInsights) request).execute();
    }
    throw new IllegalArgumentException("Request is not supported.");
  }
}
