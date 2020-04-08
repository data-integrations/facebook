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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.facebook.source.common.config.BaseSourceConfig;

import javax.annotation.Nullable;

/**
 * Provides all required configuration for reading Facebook Insights.
 */
public class FacebookBatchSourceConfig extends BaseSourceConfig {
  public static final String PROPERTY_SORTING = "sorting";
  public static final String PROPERTY_SORT_DIRECTION = "sortDirection";

  @Name(PROPERTY_SORTING)
  @Description("Field name to sort results by.")
  @Nullable
  @Macro
  protected String sorting;

  @Name(PROPERTY_SORT_DIRECTION)
  @Description("Sort direction.")
  @Nullable
  @Macro
  protected String sortDirection;

  public FacebookBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  private FacebookBatchSourceConfig(Builder builder) {
    super(builder.referenceName);
    this.sorting = builder.sorting;
    this.sortDirection = builder.sortDirection;
    this.accessToken = builder.accessToken;
    this.objectType = builder.objectType;
    this.adId = builder.adId;
    this.adSetId = builder.adSetId;
    this.campaignId = builder.campaignId;
    this.accountId = builder.accountId;
    this.fields = builder.fields;
    this.level = builder.level;
    this.filtering = builder.filtering;
    this.datePreset = builder.datePreset;
    this.breakdown = builder.breakdown;
    this.additionalBreakdown = builder.additionalBreakdown;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Nullable
  public String getSorting() {
    if (!Strings.isNullOrEmpty(sorting) && !Strings.isNullOrEmpty(sortDirection)) {
      return sorting + "_" + sortDirection;
    } else {
      return null;
    }
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);

    validateSorting(failureCollector);
  }

  void validateSorting(FailureCollector failureCollector) {
    if (!containsMacro(PROPERTY_SORTING)
      && !containsMacro(PROPERTY_SORT_DIRECTION)
      && !Strings.isNullOrEmpty(sorting)) {
      // check is direction one of "ascending" or "descending"
      if (!("ascending".equals(sortDirection) || "descending".equals(sortDirection))) {
        failureCollector
          .addFailure(
            String.format("'%s' is invalid sorting direction", sortDirection),
            "Set sorting direction to 'ascending' or 'descending'")
          .withConfigProperty(PROPERTY_SORT_DIRECTION);
      }
    }
  }

  /**
   * Builds configuration instance.
   */
  public static class Builder {
    private String referenceName;
    private String sorting;
    private String sortDirection;
    private String accessToken;
    private String objectType;
    private String adId;
    private String adSetId;
    private String campaignId;
    private String accountId;
    private String fields;
    private String level;
    private String filtering;
    private String datePreset;
    private String breakdown;
    private String additionalBreakdown;

    private Builder() {

    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setSorting(String sorting) {
      this.sorting = sorting;
      return this;
    }

    public Builder setSortDirection(String sortDirection) {
      this.sortDirection = sortDirection;
      return this;
    }

    public Builder setAccessToken(String accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    public Builder setObjectType(String objectType) {
      this.objectType = objectType;
      return this;
    }

    public Builder setAdId(String adId) {
      this.adId = adId;
      return this;
    }

    public Builder setAdSetId(String adSetId) {
      this.adSetId = adSetId;
      return this;
    }

    public Builder setCampaignId(String campaignId) {
      this.campaignId = campaignId;
      return this;
    }

    public Builder setAccountId(String accountId) {
      this.accountId = accountId;
      return this;
    }

    public Builder setFields(String fields) {
      this.fields = fields;
      return this;
    }

    public Builder setLevel(String level) {
      this.level = level;
      return this;
    }

    public Builder setFiltering(String filtering) {
      this.filtering = filtering;
      return this;
    }

    public Builder setDatePreset(String datePreset) {
      this.datePreset = datePreset;
      return this;
    }

    public Builder setBreakdown(String breakdown) {
      this.breakdown = breakdown;
      return this;
    }

    public Builder setAdditionalBreakdown(String additionalBreakdown) {
      this.additionalBreakdown = additionalBreakdown;
      return this;
    }

    public FacebookBatchSourceConfig build() {
      return new FacebookBatchSourceConfig(this);
    }
  }
}
