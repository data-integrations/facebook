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

import com.facebook.ads.sdk.APIException;
import com.facebook.ads.sdk.APINodeList;
import com.facebook.ads.sdk.AdsInsights;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequest;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequestFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

@RunWith(PowerMockRunner.class)
@PrepareForTest(fullyQualifiedNames = "io.cdap.plugin.facebook.source.common.requests.InsightsRequestFactory")
public class FacebookRecordReaderTest {
  private static final Gson gson = new GsonBuilder().create();

  @Test
  @SuppressWarnings("unchecked")
  public void testIterate() throws IOException, APIException {
    PowerMockito.mockStatic(InsightsRequestFactory.class);

    AdsInsights firstInsight = Mockito.mock(AdsInsights.class);
    AdsInsights secondInsight = Mockito.mock(AdsInsights.class);
    AdsInsights thirdInsight = Mockito.mock(AdsInsights.class);

    // simulates api, last page is empty
    APINodeList<AdsInsights> lastPageMock = (APINodeList<AdsInsights>) Mockito.mock(APINodeList.class);
    Mockito.when(lastPageMock.iterator()).thenReturn(iteratorOf());

    APINodeList<AdsInsights> secondPageMock = (APINodeList<AdsInsights>) Mockito.mock(APINodeList.class);
    Mockito.when(secondPageMock.iterator()).thenReturn(iteratorOf(thirdInsight));
    Mockito.when(secondPageMock.nextPage()).thenReturn(lastPageMock);

    APINodeList<AdsInsights> firstPageMock = (APINodeList<AdsInsights>) Mockito.mock(APINodeList.class);
    Mockito.when(firstPageMock.iterator()).thenReturn(iteratorOf(firstInsight, secondInsight));
    Mockito.when(firstPageMock.nextPage()).thenReturn(secondPageMock);

    InsightsRequest requestMock = Mockito.mock(InsightsRequest.class);
    Mockito.when(requestMock.execute()).thenReturn(firstPageMock);

    TaskAttemptContext context = Mockito.mock(TaskAttemptContext.class);
    Configuration configuration = new Configuration();
    configuration.set(
      FacebookInputFormatProvider.PROPERTY_CONFIG_JSON,
      gson.toJson(new FacebookBatchSourceConfigMock())
    );
    Mockito.when(context.getConfiguration()).thenReturn(configuration);

    PowerMockito.when(InsightsRequestFactory.createRequest(Mockito.any())).thenReturn(requestMock);


    FacebookRecordReader reader = new FacebookRecordReader();
    reader.initialize(new FacebookSplit(), context);
    Assert.assertTrue(reader.nextKeyValue());
    Assert.assertEquals(firstInsight, reader.getCurrentValue());
    Assert.assertTrue(reader.nextKeyValue());
    Assert.assertEquals(secondInsight, reader.getCurrentValue());
    Assert.assertTrue(reader.nextKeyValue());
    Assert.assertEquals(thirdInsight, reader.getCurrentValue());
    Assert.assertFalse(reader.nextKeyValue());

    Mockito.verify(firstPageMock, Mockito.times(1)).nextPage();
    Mockito.verify(secondPageMock, Mockito.times(1)).nextPage();
    Mockito.verify(lastPageMock, Mockito.times(1)).nextPage();
  }

  private <T> Iterator<T> iteratorOf(T... items) {
    return Arrays.asList(items).iterator();
  }
}

class FacebookBatchSourceConfigMock extends FacebookBatchSourceConfig {

  FacebookBatchSourceConfigMock() {
    super("ref");
    level = "ad";
    objectType = "Campaign";
    fields = "spend,age";
    accessToken = "token";
    breakdowns = "age";
    objectId = "120330000173671207";
    filtering = "[{\"field\":\"ad.impressions\",\"operator\":\"GREATER_THAN\",\"value\":0}]";
    timeRanges = "[{\"since\":\"1990-11-11\",\"until\":\"2010-11-11\"}]";
  }
}
