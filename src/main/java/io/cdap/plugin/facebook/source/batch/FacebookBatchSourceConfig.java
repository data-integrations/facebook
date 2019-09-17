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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.facebook.source.common.BaseSourceConfig;
import io.cdap.plugin.facebook.source.common.SchemaHelper;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalBreakdownException;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalInsightsFieldException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Provides all required configuration for reading Facebook Insights.
 */
public class FacebookBatchSourceConfig extends BaseSourceConfig {

  public static final String PROPERTY_FIELDS = "fields";
  public static final String PROPERTY_LEVEL = "level";
  public static final String PROPERTY_BREAKDOWNS = "breakdowns";
  public static final String PROPERTY_FILTERING = "filtering";
  public static final String PROPERTY_TIME_RANGES = "timeRanges";
  public static final String PROPERTY_SORTING = "sorting";
  /*
  Most likely unique delimiter that helps avoid problems with unescaped symbols in complex filters
   */
  public static final String FILTERING_DELIMITER = "%!delim@%";
  private static final Gson gson = new GsonBuilder().create();
  private static final Pattern OPERATOR_FIELD_RE = Pattern.compile("([(A-Z_)]+)\\(([a-z\\d_]+)\\)");

  private transient Schema schema = null;
  private transient List<Filter> filtersList = null;
  private transient String parsedFiltering = null;

  @Name(PROPERTY_FIELDS)
  @Description("Fields to get.")
  @Macro
  protected String fields;

  @Name(PROPERTY_LEVEL)
  @Description("Level of request.")
  @Macro
  protected String level;


  @Name(PROPERTY_BREAKDOWNS)
  @Description("Breakdowns to query.")
  @Nullable
  @Macro
  protected String breakdowns;

  @Name(PROPERTY_FILTERING)
  @Description("Filters to query with.")
  @Nullable
  @Macro
  protected String filtering;

  @Name(PROPERTY_TIME_RANGES)
  @Description("Time ranges.")
  @Nullable
  @Macro
  protected String timeRanges;

  @Name(PROPERTY_SORTING)
  @Description("Sorting definitions.")
  @Nullable
  @Macro
  protected String sorting;

  public FacebookBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  List<String> getFields() {
    if (!Strings.isNullOrEmpty(fields)) {
      return Arrays.asList(fields.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  Schema getSchema() {
    if (schema == null) {
      schema = SchemaHelper.buildSchema(getFields(), getBreakdowns());
    }
    return schema;
  }

  public String getLevel() {
    return level;
  }

  public List<AdsInsights.EnumBreakdowns> getBreakdowns() {
    if (!Strings.isNullOrEmpty(breakdowns)) {
      return Arrays.stream(breakdowns.split(","))
        .map(FacebookBatchSourceConfig::toBreakdown)
        .collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  @Nullable
  public String getFiltering() {
    if (!Strings.isNullOrEmpty(filtering) && parsedFiltering == null) {
      filtersList = Arrays.stream(filtering.split(FILTERING_DELIMITER))
        .map(FacebookBatchSourceConfig::parseFilteringItem)
        .collect(Collectors.toList());
      parsedFiltering = gson.toJson(filtersList);
    }
    return parsedFiltering;
  }

  private static Filter parseFilteringItem(String item) {
    // filtering item will be in format 'some random text:OPERATOR(field)'
    // everything after last ':' is the operator and field name and can't contain some special characters,
    // parse separately and consider everything before as value.
    int delimiter = item.lastIndexOf(':');
    String operatorAndField = item.substring(delimiter + 1);
    String value = item.substring(0, delimiter);
    Matcher match = OPERATOR_FIELD_RE.matcher(operatorAndField);
    if (match.matches()) {
      return new Filter(match.group(2), match.group(1), value);
    } else {
      throw new IllegalArgumentException("Failed to parse filtering");
    }
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
      .orElseThrow(() -> new IllegalBreakdownException(stringValue));
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);

    try {
      getBreakdowns();
    } catch (IllegalBreakdownException ex) {
      failureCollector
        .addFailure(ex.getMessage(), "Remove invalid breakdowns.")
        .withConfigElement(PROPERTY_BREAKDOWNS, ex.getBreakDownName());
    }

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

    try {
      if (!Strings.isNullOrEmpty(getFiltering())) {
        filtersList.forEach(filter -> {
          if (!getFields().contains(filter.field)) {
            failureCollector.addFailure(
              String.format("Invalid filter: field '%s' not specified in fields", filter.field),
              String.format("Select field '%s'", filter.field))
              .withConfigProperty(PROPERTY_FILTERING);
          }
        });
      }
    } catch (Exception ex) {
      // all kind of parsing exceptions
      failureCollector.addFailure("Failed to parse filtering:" + ex.getMessage(), null)
        .withStacktrace(ex.getStackTrace())
        .withConfigProperty(PROPERTY_FILTERING);
    }
  }

  private static class Filter {
    String field;
    String operator;
    String value;

    Filter(String field, String operator, String value) {
      this.field = field;
      this.operator = operator;
      this.value = value;
    }
  }
}
