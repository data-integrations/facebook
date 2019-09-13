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
import io.cdap.plugin.common.ReferencePluginConfig;

/**
 * Base configuration for facebook sources.
 */
public class BaseSourceConfig extends ReferencePluginConfig {
  @Name("accessToken")
  @Description("Access Token.")
  @Macro
  protected String accessToken;

  @Name("objectType")
  @Description("Object Type.")
  @Macro
  protected String objectType;

  @Name("objectId")
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

  public void validate() {
    if (!containsMacro("accessToken")) {
      if (Strings.isNullOrEmpty(accessToken)) {
        throw new IllegalArgumentException("accessToken must be not empty");
      }
    }
    if (!containsMacro("objectType")) {
      if (Strings.isNullOrEmpty(accessToken) && getObjectType() == null) {
        throw new IllegalArgumentException("objectType must be not empty");
      }
    }
    if (!containsMacro("objectId")) {
      if (Strings.isNullOrEmpty(accessToken)) {
        throw new IllegalArgumentException("objectType must be not empty");
      }
    }
  }
}
