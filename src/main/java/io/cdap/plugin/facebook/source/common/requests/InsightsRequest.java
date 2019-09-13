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
import com.facebook.ads.sdk.AdsInsights;

import java.util.List;

/**
 * Common interface for Facebook Insights requests.
 */
public interface InsightsRequest {
  void requestField(String fieldName);
  void setParam(String paramName, Object value);
  void setBreakdowns(List<AdsInsights.EnumBreakdowns> breakdowns);
  void setActionBreakdowns(List<AdsInsights.EnumActionBreakdowns> breakdowns);
  APINodeList<AdsInsights> execute() throws APIException;
}
