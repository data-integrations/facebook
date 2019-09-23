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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

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


  public BaseSourceConfig(String referenceName) {
    super(referenceName);
  }

  public String getAccessToken() {
    return accessToken;
  }

  public ObjectType getObjectType() {
    return ObjectType.fromString(objectType);
  }

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
}
