package io.cdap.plugin.facebook.source.common.config;

import com.facebook.ads.sdk.AdsInsights;

import java.util.List;

public class Breakdowns {
  List<AdsInsights.EnumBreakdowns> breakdowns;
  List<AdsInsights.EnumActionBreakdowns> actionBreakdowns;
  boolean joinableWithAction;

  public Breakdowns(List<AdsInsights.EnumBreakdowns> breakdowns,
                    List<AdsInsights.EnumActionBreakdowns> actionBreakdowns, boolean joinableWithAction) {
    this.breakdowns = breakdowns;
    this.actionBreakdowns = actionBreakdowns;
    this.joinableWithAction = joinableWithAction;
  }
}
