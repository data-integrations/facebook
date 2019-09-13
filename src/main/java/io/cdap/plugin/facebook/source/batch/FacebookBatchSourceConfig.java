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

import com.facebook.ads.sdk.AdsInsights;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.facebook.source.common.BaseSourceConfig;
import io.cdap.plugin.facebook.source.common.SchemaBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Provides all required configuration for reading Facebook Insights.
 */
public class FacebookBatchSourceConfig extends BaseSourceConfig {

  private transient Schema schema = null;

  @Name("fields")
  @Description("Fields to get.")
  @Macro
  protected String fields;

  @Name("level")
  @Description("Level of request.")
  @Macro
  protected String level;


  @Name("breakdowns")
  @Description("Breakdowns to query.")
  @Nullable
  @Macro
  protected String breakdowns;

  @Name("filtering")
  @Description("Filters to query with.")
  @Nullable
  @Macro
  protected String filtering;

  @Name("timeRanges")
  @Description("Time ranges.")
  @Nullable
  @Macro
  protected String timeRanges;

  @Name("sorting")
  @Description("Sorting definitions.")
  @Nullable
  @Macro
  protected String sorting;

  public FacebookBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  List<String> getFields() {
    return Arrays.asList(fields.split(","));
  }

  Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(getFields());
    }
    return schema;
  }

  public String getLevel() {
    return level;
  }

  public List<AdsInsights.EnumBreakdowns> getBreakdowns() {
    if (!Strings.isNullOrEmpty(breakdowns)) {
      return StreamSupport.stream(Arrays.spliterator(breakdowns.split(",")), false)
        .map(FacebookBatchSourceConfig::toBreakdown)
        .collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  @Nullable
  public String getFiltering() {
    return filtering;
  }

  @Nullable
  public String getTimeRanges() {
    return timeRanges;
  }

  @Nullable
  public String getSorting() {
    return sorting;
  }

  private static AdsInsights.EnumBreakdowns toBreakdown(String stringValue) {
    return StreamSupport
      .stream(Arrays.spliterator(AdsInsights.EnumBreakdowns.values()), false)
      .filter(enumBreakdown -> Objects.equals(enumBreakdown.toString(), stringValue))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(String.format("'%s' is illegal breakdown", stringValue)));
  }

  @Override
  public void validate() {
    super.validate();
    if (!containsMacro("fields")) {
      if (Strings.isNullOrEmpty(fields) && getFields().size() == 0) {
        throw new IllegalArgumentException("at least one field must be specified");
      }
    }

    if (!containsMacro("breakdowns")) {
      getBreakdowns();
    }
  }
}
