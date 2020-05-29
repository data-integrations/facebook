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
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.facebook.source.common.SchemaHelper;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalBreakdownException;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalInsightsFieldException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Base configuration for facebook sources.
 */
public class BaseSourceConfig extends ReferencePluginConfig {
  public static final String PROPERTY_ACCESS_TOKEN = "accessToken";
  public static final String PROPERTY_OBJECT_TYPE = "objectType";
  public static final String PROPERTY_AD_ID = "adId";
  public static final String PROPERTY_AD_SET_ID = "adSetId";
  public static final String PROPERTY_CAMPAIGN_ID = "campaignId";
  public static final String PROPERTY_ACCOUNT_ID = "accountId";
  public static final String PROPERTY_FIELDS = "fields";
  public static final String PROPERTY_LEVEL = "level";
  public static final String PROPERTY_FILTERING = "filtering";
  public static final String PROPERTY_DATE_PRESET = "datePreset";
  public static final String PROPERTY_BREAKDOWN = "breakdown";
  public static final String PROPERTY_ADDITIONAL_BREAKDOWN = "additionalBreakdown";

  @Name(PROPERTY_ACCESS_TOKEN)
  @Description("Access Token.")
  @Macro
  protected String accessToken;

  @Name(PROPERTY_OBJECT_TYPE)
  @Description("Object Type.")
  @Macro
  protected String objectType;

  @Name(PROPERTY_AD_ID)
  @Description("Ad Id.")
  @Macro
  @Nullable
  protected String adId;

  @Name(PROPERTY_AD_SET_ID)
  @Description("Ad Set Id.")
  @Macro
  @Nullable
  protected String adSetId;

  @Name(PROPERTY_CAMPAIGN_ID)
  @Description("Campaign Id.")
  @Macro
  @Nullable
  protected String campaignId;

  @Name(PROPERTY_ACCOUNT_ID)
  @Description("Account Id.")
  @Macro
  @Nullable
  protected String accountId;

  @Name(PROPERTY_FIELDS)
  @Description("Fields to get.")
  @Macro
  protected String fields;

  @Name(PROPERTY_LEVEL)
  @Description("Level of request.")
  @Macro
  protected String level;

  @Name(PROPERTY_FILTERING)
  @Description("Filters to query with.")
  @Nullable
  @Macro
  protected String filtering;

  @Name(PROPERTY_DATE_PRESET)
  @Description("Time rage to query results.")
  @Macro
  protected String datePreset;

  @Name(PROPERTY_BREAKDOWN)
  @Description("Primary breakdown.")
  @Nullable
  @Macro
  protected String breakdown;

  @Name(PROPERTY_ADDITIONAL_BREAKDOWN)
  @Description("Additional breakdown. Can be selected with primary breakdowns marked by '*'.")
  @Nullable
  @Macro
  protected String additionalBreakdown;

  /*
  Most likely unique delimiter that helps avoid problems with unescaped symbols in complex filters
  */
  public static final String FILTERING_DELIMITER = "%!delim@%";
  private static final Gson gson = new GsonBuilder().create();

  private transient Schema schema = null;

  public BaseSourceConfig(String referenceName) {
    super(referenceName);
  }

  public String getAccessToken() {
    return accessToken;
  }

  public ObjectType getObjectType() {
    return ObjectType.fromString(objectType);
  }

  /**
   * Returns the object id depending on the object type set.
   * @return The string value of either campaignId / AdId / adSetId / accountId
   */
  public String getObjectId() {
    switch (getObjectType()) {
      case Campaign:
        return campaignId;
      case Ad:
        return adId;
      case AdSet:
        return adSetId;
      case Account:
        return accountId;
      default:
        throw new IllegalArgumentException("Unknown object type");
    }
  }

  /**
   * Returns list of field names.
   * @return the list of fields
   */
  public List<String> getFields() {
    if (!Strings.isNullOrEmpty(fields)) {
      return Arrays.asList(fields.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Returns selected Schema.
   * @return instance of Schema
   */
  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaHelper.buildSchema(getFields(), getBreakdown());
    }
    return schema;
  }

  public String getLevel() {
    return level;
  }

  /**
   * Returns selected Filtering.
   * @return  String of Filtering
   */
  @Nullable
  public String getFiltering() {
    if (!Strings.isNullOrEmpty(filtering)) {
      return gson.toJson(getFilters());
    } else {
      return null;
    }
  }

  /**
   * Returns selected Filters.
   * @return  The list of Filter
   */
  public List<Filter> getFilters() {
    if (!Strings.isNullOrEmpty(filtering)) {
      return Arrays.stream(filtering.split(FILTERING_DELIMITER))
        .map(SourceConfigHelper::parseFilteringItem)
        .collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public String getDatePreset() {
    return datePreset;
  }

  /**
   * Returns selected Breakdown.
   * @return   The instance of Breakdowns
   */
  public Breakdowns getBreakdown() {
    if (!Strings.isNullOrEmpty(breakdown) && !"none".equals(breakdown)) {
      Breakdowns result = SourceConfigHelper.parseBreakdowns(breakdown);
      if (!Strings.isNullOrEmpty(additionalBreakdown) && !"none".equals(additionalBreakdown)) {
        AdsInsights.EnumActionBreakdowns additionalActionBreakdown =
          SourceConfigHelper.actionBreakdownFromString(additionalBreakdown);
        if (result.isJoinableWithAction() && result.getActionBreakdowns().contains(additionalActionBreakdown)) {
          result.getActionBreakdowns().add(additionalActionBreakdown);
        }
      }
      return result;
    } else {
      return null;
    }
  }

  /**
   * Validates {@link BaseSourceConfig} instance.
   */
  public void validate(FailureCollector failureCollector) {
    if (!containsMacro(PROPERTY_ACCESS_TOKEN) && Strings.isNullOrEmpty(accessToken)) {
      failureCollector
        .addFailure("accessToken must be not empty.", "Enter valid access token.")
        .withConfigProperty(PROPERTY_ACCESS_TOKEN);
    }

    if (!containsMacro(PROPERTY_OBJECT_TYPE)) {
      try {
        getObjectType();
      } catch (IllegalArgumentException ex) {
        failureCollector
          .addFailure(
            ex.getMessage(),
            "Choose one of 'Campaign', 'Ad', 'Ad Set' or 'Account'.")
          .withConfigProperty(PROPERTY_OBJECT_TYPE);
      }
    }

    validateObjectId(failureCollector);
    validateBreakdowns(failureCollector);
    validateFields(failureCollector);
    validateFiltering(failureCollector);
    validateDatePreset(failureCollector);
  }

  void validateObjectId(FailureCollector failureCollector) {
    if (!containsMacro(PROPERTY_OBJECT_TYPE)) {
      switch (getObjectType()) {
        case Campaign:
          if (!containsMacro(PROPERTY_CAMPAIGN_ID) && Strings.isNullOrEmpty(campaignId)) {
            failureCollector
              .addFailure("Campaign Id must be not empty.", "Enter valid Campaign Id.")
              .withConfigProperty(PROPERTY_CAMPAIGN_ID);
          }
          break;
        case Ad:
          if (!containsMacro(PROPERTY_AD_ID) && Strings.isNullOrEmpty(adId)) {
            failureCollector
              .addFailure("Ad Id must be not empty.", "Enter valid Ad Id.")
              .withConfigProperty(PROPERTY_AD_ID);
          }
          break;
        case AdSet:
          if (!containsMacro(PROPERTY_AD_SET_ID) && Strings.isNullOrEmpty(adSetId)) {
            failureCollector
              .addFailure("Ad Set Id must be not empty.", "Enter valid Ad Set Id.")
              .withConfigProperty(PROPERTY_AD_SET_ID);
          }
          break;
        case Account:
          if (!containsMacro(PROPERTY_ACCOUNT_ID) && Strings.isNullOrEmpty(accountId)) {
            failureCollector
              .addFailure("Account Id must be not empty.", "Enter valid Account Id.")
              .withConfigProperty(PROPERTY_ACCOUNT_ID);
          }
          break;
      }
    }
  }

  void validateFields(FailureCollector failureCollector) {
    if (!containsMacro(PROPERTY_FIELDS)) {
      if (Strings.isNullOrEmpty(fields) && getFields().size() == 0) {
        failureCollector
          .addFailure("At least one field must be specified.", "Specify valid fields.")
          .withConfigProperty(PROPERTY_FIELDS);
      } else {
        getFields().forEach(field -> {
          try {
            SchemaHelper.fromName(field);
          } catch (IllegalInsightsFieldException ex) {
            failureCollector
              .addFailure("Invalid field:" + ex.getFieldName(), "Remove invalid field.")
              .withConfigElement(PROPERTY_FIELDS, ex.getFieldName());
          }
        });
      }
    }
  }

  void validateFiltering(FailureCollector failureCollector) {
    if (!containsMacro(PROPERTY_FILTERING)) {
      try {
        getFiltering();
      } catch (Exception ex) {
        // all kind of parsing exceptions
        failureCollector.addFailure("Failed to parse filtering:" + ex.getMessage(), null)
          .withStacktrace(ex.getStackTrace())
          .withConfigProperty(PROPERTY_FILTERING);
      }
    }
  }

  void validateDatePreset(FailureCollector failureCollector) {
    if (!SourceConfigHelper.isValidDatePreset(getDatePreset())) {
      failureCollector.addFailure(String.format("'%s' is not a valid date preset", getDatePreset()), null)
        .withConfigProperty(PROPERTY_DATE_PRESET);
    }
  }

  void validateBreakdowns(FailureCollector failureCollector) {
    if (!containsMacro(PROPERTY_BREAKDOWN)) {
      try {
        getBreakdown();
      } catch (IllegalBreakdownException ex) {
        failureCollector
          .addFailure(ex.getMessage(), "Fix invalid breakdown value.")
          .withConfigProperty(PROPERTY_BREAKDOWN);
      }
    }
  }
}
