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
package com.hotels.bdp.circustrain.bigquery.executor;

import static jodd.util.StringUtil.isBlank;

import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.http.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryUtils;

@Experimental
public class TempTableQueryExecutor {

  private static final Logger log = LoggerFactory.getLogger(TempTableQueryExecutor.class);

  private final BigQuery bigQuery;
  private final String datasetName;
  private final String tableName;

  public TempTableQueryExecutor(BigQuery bigQuery, String dataset, String table) {
    this.bigQuery = bigQuery;
    this.datasetName = dataset;
    this.tableName = table;
  }

  // TODO: Ensure that Dataset and Table match those specified in config as this will be used to overwrite the source
  public com.google.cloud.bigquery.Table execute(String query) {
    if (isBlank(query)) {
      return null;
    }
    log.info("Executing query {} and storing the results in {}.{}", query, datasetName, tableName);
    QueryJobConfiguration queryConfig = QueryJobConfiguration
        .newBuilder(query)
        .setDestinationTable(TableId.of(datasetName, tableName))
        .setUseLegacySql(true)
        .setAllowLargeResults(true)
        .build();

    log.debug("QueryJobConfiguration: {}", queryConfig);

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    try {
      queryJob = queryJob.waitFor();
    } catch (InterruptedException e) {
      throw new CircusTrainException(String.format("Query: %s failed."), e);
    }

    if (queryJob == null) {
      throw new CircusTrainException(String.format("Query: %s failed.", query));
    } else if (queryJob.getStatus().getError() != null) {
      throw new CircusTrainException(
          String.format("Query: %s failed.\n%s", query, queryJob.getStatus().getError().toString()));
    }
    try {
      return BigQueryUtils.getBigQueryTable(bigQuery, datasetName, tableName);
    } catch (NoSuchObjectException e) {
      throw new CircusTrainException(e);
    }
  }
}
