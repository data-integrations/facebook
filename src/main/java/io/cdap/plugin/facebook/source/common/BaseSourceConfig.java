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

package io.cdap.plugin.facebook.source.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

/**
 * Base configuration for facebook sources.
 */
public class BaseSourceConfig extends ReferencePluginConfig {
  public static final String PROPERTY_ACCESS_TOKEN = "accessToken";
  public static final String PROPERTY_OBJECT_TYPE = "objectType";
  public static final String PROPERTY_OBJECT_ID = "objectId";

  @Name(PROPERTY_ACCESS_TOKEN)
  @Description("Access Token.")
  @Macro
  protected String accessToken;

  @Name(PROPERTY_OBJECT_TYPE)
  @Description("Object Type.")
  @Macro
  protected String objectType;

  @Name(PROPERTY_OBJECT_ID)
  @Description("Object Id.")
  @Macro
  protected String objectId;

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
    return objectId;
  }

  public void validate(FailureCollector failureCollector) {
    if (Strings.isNullOrEmpty(accessToken)) {
      failureCollector
        .addFailure("accessToken must be not empty.", "Enter valid access token.")
        .withConfigProperty(PROPERTY_ACCESS_TOKEN);
    }

    try {
      getObjectType();
    } catch (IllegalArgumentException ex) {
      failureCollector
        .addFailure(
          ex.getMessage(),
          "Choose one of 'Campaign', 'Ad', 'Ad Set' or 'Account'.")
        .withConfigProperty(PROPERTY_OBJECT_TYPE);
    }

    if (Strings.isNullOrEmpty(objectId)) {
      failureCollector
        .addFailure("objectId must be not empty.", "Enter valid object id.")
        .withConfigProperty(PROPERTY_OBJECT_ID);
    }
  }
}
