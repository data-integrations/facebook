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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.facebook.source.common.SchemaHelper;
import io.cdap.plugin.facebook.source.common.config.BaseSourceConfig;
import io.cdap.plugin.facebook.source.common.config.Breakdowns;
import io.cdap.plugin.facebook.source.common.config.SourceConfigHelper;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalBreakdownException;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalInsightsFieldException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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

  private transient Schema schema = null;

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
      // TODO fix this
      schema = SchemaHelper.buildSchema(getFields(), getBreakdowns());
    }
    return schema;
  }

  public String getLevel() {
    return level;
  }

  public Breakdowns getBreakdowns() {
    if (!Strings.isNullOrEmpty(breakdowns)) {
      return SourceConfigHelper.parseBreakdowns(breakdowns);
    } else {
      return null;
    }
  }

  @Nullable
  public String getFiltering() {
    if (!Strings.isNullOrEmpty(filtering)) {
      return gson.toJson(
        Arrays.stream(filtering.split(FILTERING_DELIMITER))
          .map(SourceConfigHelper::parseFilteringItem)
          .collect(Collectors.toList())
      );
    } else {
      return null;
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

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);

    validateBreakdowns(failureCollector);
    validateFields(failureCollector);
    validateFiltering(failureCollector);
  }

  void validateBreakdowns(FailureCollector failureCollector) {
    try {
      getBreakdowns();
    } catch (IllegalBreakdownException ex) {
      failureCollector
        .addFailure(ex.getMessage(), "Remove invalid breakdowns.")
        .withConfigProperty(PROPERTY_BREAKDOWNS);
    }
  }

  void validateFields(FailureCollector failureCollector) {
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

  void validateFiltering(FailureCollector failureCollector) {
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
