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

package io.cdap.plugin.facebook.source;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;

import java.util.List;
import java.util.stream.Collectors;

public class BaseFacebookValidationTest {
  protected static void assertSingleFieldValidationFailed(MockFailureCollector failureCollector, String paramName) {
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = failureCollector.getValidationFailures().get(0).getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(paramName, causeList.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
