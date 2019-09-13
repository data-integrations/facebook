/*
 * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.facebook.source.common.config.SourceConfigHelper;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalInsightsFieldException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SchemaHelperTest {

  @Test
  public void buildSchema() {
    Schema expectedSchema = Schema.recordOf(
      "TestSchema",
      Schema.Field.of("ad_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("gender", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("actions_results", Schema.nullableOf(SchemaHelper.createAddActionStatsSchema())),
      Schema.Field.of("actions", Schema.nullableOf(Schema.arrayOf(SchemaHelper.createAddActionStatsSchema()))));

    Schema resultingSchema = SchemaHelper.buildSchema(
      Arrays.asList("ad_id", "actions_results", "actions"), // age and gender will be added from breakdown
      SourceConfigHelper.parseBreakdowns("age, gender *")
    );

    Assert.assertTrue(expectedSchema.isCompatible(resultingSchema));
  }

  @Test(expected = IllegalInsightsFieldException.class)
  public void buildSchemaInvalidField() {
    SchemaHelper.buildSchema(Arrays.asList("ad_id", "actions_results", "actions", "invalid"), null);
  }

  @Test
  public void fieldNameToSchemaName() {
    Assert.assertEquals("view_7d", SchemaHelper.fieldNameToSchemaName("7d_view"));
    Assert.assertEquals("not_mapped", SchemaHelper.fieldNameToSchemaName("not_mapped"));
  }
}
