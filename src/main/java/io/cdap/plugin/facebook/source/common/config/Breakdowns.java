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

import java.util.List;

/**
 * Represents breakdowns for request.
 */
public class Breakdowns {
  List<AdsInsights.EnumBreakdowns> breakdowns;
  List<AdsInsights.EnumActionBreakdowns> actionBreakdowns;
  boolean joinableWithAction;

  /**
   * Constructor for Breakdowns object.
   * @param breakdowns            The breakdowns
   * @param actionBreakdowns      The action breakdowns
   * @param joinableWithAction    The joinable with action
   */
  public Breakdowns(List<AdsInsights.EnumBreakdowns> breakdowns,
                    List<AdsInsights.EnumActionBreakdowns> actionBreakdowns, boolean joinableWithAction) {
    this.breakdowns = breakdowns;
    this.actionBreakdowns = actionBreakdowns;
    this.joinableWithAction = joinableWithAction;
  }

  public List<AdsInsights.EnumBreakdowns> getBreakdowns() {
    return breakdowns;
  }

  public List<AdsInsights.EnumActionBreakdowns> getActionBreakdowns() {
    return actionBreakdowns;
  }

  public boolean isJoinableWithAction() {
    return joinableWithAction;
  }
}
