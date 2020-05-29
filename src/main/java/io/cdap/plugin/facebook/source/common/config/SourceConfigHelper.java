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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalBreakdownException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class with helper methods to parse input configuration.
 */
public class SourceConfigHelper {

  private static final Pattern OPERATOR_FIELD_RE = Pattern.compile("([(A-Z_)]+)\\((\\S+)\\)");

  /**
   * Returns selected Breakdowns.
   * @param breakdownsString The breakdowns string
   * @return   The instance of Breakdowns
   */
  public static Breakdowns parseBreakdowns(String breakdownsString) {
    switch (breakdownsString) {
      /*
      case "action_converted_product_id":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CONVERTED_PRODUCT_ID),
          false
        );
      */
      case "action_type *":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_TYPE),
          true
        );
      /*
      case "action_type, action_converted_product_id":
        return new Breakdowns(
          Collections.emptyList(),
          Arrays.asList(
            AdsInsights.EnumActionBreakdowns.VALUE_ACTION_TYPE,
            AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CONVERTED_PRODUCT_ID
          ),
          false
        );
      */
      case "action_target_id *":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_TARGET_ID),
          true
        );
      case "action_device *":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DEVICE),
          true
        );
      case "action_device, impression_device *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DEVICE),
          true
        );
      case "action_device, publisher_platform *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DEVICE),
          true
        );
      case "action_device, publisher_platform, impression_device *":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM,
            AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE
          ),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DEVICE),
          true
        );
      case "action_device, publisher_platform, platform_position *":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM,
            AdsInsights.EnumBreakdowns.VALUE_PLATFORM_POSITION
          ),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DEVICE),
          true
        );
      case "action_device, publisher_platform, platform_position, impression_device *":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM,
            AdsInsights.EnumBreakdowns.VALUE_PLATFORM_POSITION,
            AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE
          ),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DEVICE),
          true
        );
      case "action_reaction":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_REACTION),
          false
        );
      case "action_type, action_reaction":
        return new Breakdowns(
          Collections.emptyList(),
          Arrays.asList(
            AdsInsights.EnumActionBreakdowns.VALUE_ACTION_TYPE,
            AdsInsights.EnumActionBreakdowns.VALUE_ACTION_REACTION
          ),
          false
        );
      case "age *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_AGE),
          Collections.emptyList(),
          true
        );
      case "gender *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_GENDER),
          Collections.emptyList(),
          true
        );
      case "age, gender *":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_AGE,
            AdsInsights.EnumBreakdowns.VALUE_GENDER
          ),
          Collections.emptyList(),
          true
        );
      case "country *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_COUNTRY),
          Collections.emptyList(),
          true
        );
      case "region *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_REGION),
          Collections.emptyList(),
          true
        );
      case "publisher_platform *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM),
          Collections.emptyList(),
          true
        );
      case "publisher_platform, impression_device *":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM,
            AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE
          ),
          Collections.emptyList(),
          true
        );
      case "publisher_platform, platform_position *":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM,
            AdsInsights.EnumBreakdowns.VALUE_PLATFORM_POSITION
          ),
          Collections.emptyList(),
          true
        );
      case "publisher_platform, platform_position, impression_device *":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM,
            AdsInsights.EnumBreakdowns.VALUE_PLATFORM_POSITION,
            AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE
          ),
          Collections.emptyList(),
          true
        );
      case "product_id *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_PRODUCT_ID),
          Collections.emptyList(),
          true
        );
      case "hourly_stats_aggregated_by_advertiser_time_zone *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_HOURLY_STATS_AGGREGATED_BY_ADVERTISER_TIME_ZONE),
          Collections.emptyList(),
          true
        );
      case "hourly_stats_aggregated_by_audience_time_zone *":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_HOURLY_STATS_AGGREGATED_BY_AUDIENCE_TIME_ZONE),
          Collections.emptyList(),
          true
        );
      case "action_carousel_card_id":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_ID),
          false
        );
      case "action_carousel_card_name":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_NAME),
          false
        );
      case "action_carousel_card_id, impression_device":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_ID),
          false
        );
      case "action_carousel_card_id, country":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_COUNTRY),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_ID),
          false
        );
      case "action_carousel_card_id, age":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_AGE),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_ID),
          false
        );
      case "action_carousel_card_id, gender":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_GENDER),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_ID),
          false
        );
      case "action_carousel_card_id, age, gender":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_AGE,
            AdsInsights.EnumBreakdowns.VALUE_GENDER
          ),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_ID),
          false
        );
      case "action_carousel_card_name, impression_device":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_NAME),
          false
        );
      case "action_carousel_card_name, country":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_COUNTRY),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_NAME),
          false
        );
      case "action_carousel_card_name, age":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_AGE),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_NAME),
          false
        );
      case "action_carousel_card_name, gender":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_GENDER),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_NAME),
          false
        );
      case "action_carousel_card_name, age, gender":
        return new Breakdowns(
          Arrays.asList(
            AdsInsights.EnumBreakdowns.VALUE_AGE,
            AdsInsights.EnumBreakdowns.VALUE_GENDER
          ),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CAROUSEL_CARD_NAME),
          false
        );
      case "ad_format_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_AD_FORMAT_ASSET),
          Collections.emptyList(),
          false
        );
      case "body_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_BODY_ASSET),
          Collections.emptyList(),
          false
        );
      case "call_to_action_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_CALL_TO_ACTION_ASSET),
          Collections.emptyList(),
          false
        );
      case "description_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_DESCRIPTION_ASSET),
          Collections.emptyList(),
          false
        );
      case "image_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_IMAGE_ASSET),
          Collections.emptyList(),
          false
        );
      case "impression_device":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE),
          Collections.emptyList(),
          false
        );
      case "link_url_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_LINK_URL_ASSET),
          Collections.emptyList(),
          false
        );
      case "title_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_TITLE_ASSET),
          Collections.emptyList(),
          false
        );
      case "video_asset":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_VIDEO_ASSET),
          Collections.emptyList(),
          false
        );
      case "dma":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_DMA),
          Collections.emptyList(),
          false
        );
      case "frequency_value":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_FREQUENCY_VALUE),
          Collections.emptyList(),
          false
        );
      case "place_page_id":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_PLACE_PAGE_ID),
          Collections.emptyList(),
          false
        );
      case "platform_position":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_PLATFORM_POSITION),
          Collections.emptyList(),
          false
        );
      case "device_platform":
        return new Breakdowns(
          Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_DEVICE_PLATFORM),
          Collections.emptyList(),
          false
        );
      case "action_canvas_component_name":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_CANVAS_COMPONENT_NAME),
          false
        );
      case "action_destination":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DESTINATION),
          false
        );
      case "action_video_sound":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_VIDEO_SOUND),
          false
        );
      case "action_video_type":
        return new Breakdowns(
          Collections.emptyList(),
          Collections.singletonList(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_VIDEO_TYPE),
          false
        );
      default:
        throw new IllegalBreakdownException("Invalid breakdowns combination:" + breakdownsString);
    }
  }

  /**
   * Returns selected EnumActionBreakdowns.
   * @param breakdown The breakdown
   * @return   The EnumActionBreakdowns of  AdsInsights
   */
  public static AdsInsights.EnumActionBreakdowns actionBreakdownFromString(String breakdown) {
    Preconditions.checkNotNull(breakdown, "breakdown must not be null");
    return Arrays.stream(AdsInsights.EnumActionBreakdowns.values())
      .filter(actionBreakdown -> breakdown.equals(actionBreakdown.toString()))
      .findFirst()
      .orElseThrow(
        () -> new IllegalArgumentException(String.format("unknown action breakdown '%s'", breakdown))
      );
  }

  /**
   * Returns selected Filter.
   * @param item The item
   * @return The instance of Filter
   */
  public static Filter parseFilteringItem(String item) {
    // filtering item will be in format 'some random text:OPERATOR(field)'
    // everything after last ':' is the operator and field name and can't contain some special characters,
    // parse separately and consider everything before as value.
    int delimiter = item.lastIndexOf(':');
    String operatorAndField = item.substring(delimiter + 1);
    String value = item.substring(0, delimiter);
    Matcher match = OPERATOR_FIELD_RE.matcher(operatorAndField);
    if (match.matches()) {
      String operator = match.group(1);
      if (!validOperators.contains(operator)) {
        throw new IllegalArgumentException(String.format("invalid filter operator '%s'", operator));
      }
      return new Filter(match.group(2), match.group(1), value);
    } else {
      throw new IllegalArgumentException("filtering string does not match pattern 'value:OPERATOR(field)'");
    }
  }

  public static boolean isValidDatePreset(String datePreset) {
    return validDatePresets.contains(datePreset);
  }

  private static final List<String> validOperators = ImmutableList.of(
    "EQUAL", "NOT_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL", "LESS_THAN", "LESS_THAN_OR_EQUAL",
    "IN_RANGE", "NOT_IN_RANGE", "CONTAIN", "NOT_CONTAIN", "NOT_IN", "STARTS_WITH", "ANY", "ALL", "AFTER",
    "BEFORE", "NONE"
  );

  private static final List<String> validDatePresets = ImmutableList.of(
    "today", "yesterday", "this_week_sun_today", "this_week_mon_today", "last_week_sun_sat", "last_week_mon_sun",
    "this_month", "last_month", "this_quarter", "last_3d", "last_7d", "last_14d", "last_28d", "last_30d", "last_90d",
    "this_year", "last_year", "lifetime"
  );
}
