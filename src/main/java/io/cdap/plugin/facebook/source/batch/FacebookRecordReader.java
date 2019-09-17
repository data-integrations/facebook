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
import io.cdap.plugin.facebook.source.common.SchemaHelper;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequest;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequestFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * RecordReader implementation, which reads {@link AdsInsights} instances from Facebook Insights using
 * facebook-java-business-sdk.
 */
public class FacebookRecordReader extends RecordReader<NullWritable, AdsInsights> {
  private static final Gson gson = new GsonBuilder().create();
  private APINodeList<AdsInsights> currentPage;
  private Iterator<AdsInsights> currentPageIterator;
  private AdsInsights currentInsight;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(FacebookInputFormatProvider.PROPERTY_CONFIG_JSON);
    FacebookBatchSourceConfig fbConfig = gson.fromJson(configJson, FacebookBatchSourceConfig.class);

    try {
      InsightsRequest request = InsightsRequestFactory.createRequest(fbConfig);
      List<String> fieldsToQuery = fbConfig.getFields()
        .stream()
        .filter(SchemaHelper::isValidForFieldsParameter)
        .collect(Collectors.toList());
      fieldsToQuery.forEach(request::requestField);
      request.setBreakdowns(fbConfig.getBreakdowns());
      if (fbConfig.getFiltering() != null) {
        request.setParam("filtering", fbConfig.getFiltering());
      }
      if (fbConfig.getSorting() != null) {
        request.setParam("sort", fbConfig.getSorting());
      }
      if (fbConfig.getTimeRanges() != null) {
        request.setParam("time_ranges", fbConfig.getTimeRanges());
      }
      if (!"default".equals(fbConfig.getLevel())) {
        request.setParam("level", fbConfig.getLevel());
      }
      currentPage = request.execute();
      currentPageIterator = currentPage.iterator();
    } catch (APIException e) {
      throw new IOException(e.getMessage(), e);
    }
  }


  @Override
  public boolean nextKeyValue() throws IOException {
    if (!currentPageIterator.hasNext()) {
      try {
        // switch page
        APINodeList<AdsInsights> nextPage = currentPage.nextPage();
        if (nextPage != null) {
          currentPage = nextPage;
          currentPageIterator = currentPage.iterator();
          return nextKeyValue();
        }
      } catch (APIException e) {
        throw new IOException(e.getMessage(), e);
      }
      return false;
    } else {
      currentInsight = currentPageIterator.next();
      return true;
    }
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public AdsInsights getCurrentValue() {
    return currentInsight;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() {

  }
}
