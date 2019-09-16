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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalInsightsFieldException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SchemaBuilderTest {

  @Test
  public void buildSchema() {
    Schema expectedSchema = Schema.recordOf(
      "TestSchema",
      Schema.Field.of("ad_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("actions_results", Schema.nullableOf(SchemaBuilder.createAddActionStatsSchema())),
      Schema.Field.of("actions", Schema.nullableOf(Schema.arrayOf(SchemaBuilder.createAddActionStatsSchema()))));

    Schema resultingSchema = SchemaBuilder.buildSchema(Arrays.asList("ad_id", "actions_results", "actions"));

    Assert.assertTrue(expectedSchema.isCompatible(resultingSchema));
  }

  @Test(expected = IllegalInsightsFieldException.class)
  public void buildSchemaInvalidField() {
    SchemaBuilder.buildSchema(Arrays.asList("ad_id", "actions_results", "actions", "invalid"));
  }

  @Test
  public void fieldNameToSchemaName() {
    Assert.assertEquals("view_7d", SchemaBuilder.fieldNameToSchemaName("7d_view"));
    Assert.assertEquals("not_mapped", SchemaBuilder.fieldNameToSchemaName("not_mapped"));
  }
}
