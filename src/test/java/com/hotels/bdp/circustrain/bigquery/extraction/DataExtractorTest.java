/**
 * Copyright (C) 2018 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.circustrain.bigquery.extraction;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class DataExtractorTest {

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private DataExtractor extractor;
  private @Mock Storage storage;
  private @Mock Table table;
  private @Mock Job job;
  private @Mock JobStatus jobStatus;

  @Before
  public void init() {
    extractor = new DataExtractor(storage);
  }

  @Test
  public void extract() throws InterruptedException {
    ExtractionUri data = new ExtractionUri();
    TableId tableId = TableId.of("dataset", "table");
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(table.getTableId()).thenReturn(tableId);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(job);
    when(job.getStatus()).thenReturn(jobStatus);
    when(jobStatus.getError()).thenReturn(null);

    extractor.add(new ExtractionContainer(table, data, false));
    extractor.extract();

    verify(storage).create(any(BucketInfo.class));
    verify(table).extract(eq("csv"), eq(data.getUri()));
  }

  @Test
  public void jobNoLongerExists() throws InterruptedException {
    expectedException.expect(CircusTrainException.class);
    expectedException.expectMessage("job no longer exists");

    ExtractionUri data = new ExtractionUri();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(null);

    extractor.add(new ExtractionContainer(table, data, false));
    extractor.extract();
  }

  @Test
  public void jobError() throws InterruptedException {
    BigQueryError bigQueryError = new BigQueryError("reason", "getExtractedDataBaseLocation", "message");
    expectedException.expect(CircusTrainException.class);
    expectedException.expectMessage(bigQueryError.getReason());
    expectedException.expectMessage(bigQueryError.getLocation());
    expectedException.expectMessage(bigQueryError.getMessage());

    ExtractionUri data = new ExtractionUri();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(job);
    when(job.getStatus()).thenReturn(jobStatus);
    when(jobStatus.getError()).thenReturn(new BigQueryError("reason", "getExtractedDataBaseLocation", "message"));

    extractor.add(new ExtractionContainer(table, data, false));
    extractor.extract();
  }

}