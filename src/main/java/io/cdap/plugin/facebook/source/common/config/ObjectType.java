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

import java.util.Arrays;

/**
 * Convenience enum to map UI selections to meaningful values.
 */
public enum ObjectType {
  Campaign("Campaign"),
  Ad("Ad"),
  AdSet("Ad Set"),
  Account("Account");

  private String stringValue;

  ObjectType(String stringValue) {
    this.stringValue = stringValue;
  }

  /**
   * Returns selected ObjectType.
   * @param value The value
   * @return  The instance of ObjectType
   */
  public static ObjectType fromString(String value) {
    return Arrays.stream(ObjectType.values())
      .filter(type -> type.stringValue.equals(value))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(String.format("'%s' is invalid ObjectType.", value)));
  }
}
