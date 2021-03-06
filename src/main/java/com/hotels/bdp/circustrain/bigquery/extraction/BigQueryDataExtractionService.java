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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class BigQueryDataExtractionService {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionService.class);

  private final Storage storage;

  public BigQueryDataExtractionService(Storage storage) {
    this.storage = storage;
  }

  void extract(Table table, BigQueryExtractionData extractionData) {
    createBucket(extractionData);
    extractDataFromBigQuery(extractionData, table);
  }

  void cleanup(BigQueryExtractionData extractionData) {
    deleteBucketAndContents(extractionData.getDataBucket());
  }

  private void deleteBucketAndContents(String dataBucket) {
    deleteObjectsInBucket(dataBucket);
    deleteBucket(dataBucket);
  }

  private void deleteObjectsInBucket(String dataBucket) {
    try {
      Iterable<Blob> blobs = storage.list(dataBucket).iterateAll();
      for (Blob blob : blobs) {
        try {
          boolean suceeded = storage.delete(blob.getBlobId());
          if (suceeded) {
            log.info("Deleted object {}", blob);
          } else {
            log.warn("Could not delete object {}", blob);
          }
        } catch (StorageException e) {
          log.warn("Error deleting object {} in bucket {}", blob, dataBucket, e);
        }
      }
    } catch (StorageException e) {
      log.warn("Error fetching objects in bucket {} for deletion", dataBucket, e);
    }
  }

  private void deleteBucket(String dataBucket) {
    try {
      Bucket bucket = storage.get(dataBucket);
      boolean suceeded = bucket.delete();
      if (suceeded) {
        log.info("Deleted bucket {}", dataBucket);
      } else {
        log.warn("Could not delete bucket {}", dataBucket);
      }
    } catch (StorageException e) {
      log.warn("Error deleting bucket {}", dataBucket, e);
    }
  }

  private void createBucket(BigQueryExtractionData extractionData) {
    String dataBucket = extractionData.getDataBucket();
    log.info("Creating bucket {}", dataBucket);
    BucketInfo bucketInfo = BucketInfo.of(dataBucket);
    storage.create(bucketInfo);
  }

  private void extractDataFromBigQuery(BigQueryExtractionData extractionData, Table table) {
    String dataset = table.getTableId().getDataset();
    String tableName = table.getTableId().getTable();
    String format = extractionData.getFormat();
    String dataUri = extractionData.getDataUri();

    log.info("Extracting {}.{} to temporary location {}", dataset, tableName, dataUri);
    try {
      Job job = table.extract(format, dataUri);
      Job completedJob = job.waitFor();
      if (completedJob == null) {
        throw new CircusTrainException("Error extracting BigQuery table data to Google storage, job no longer exists");
      } else if (completedJob.getStatus().getError() != null) {
        BigQueryError error = completedJob.getStatus().getError();
        throw new CircusTrainException("Error extracting BigQuery table data to Google storage: "
            + error.getMessage()
            + ", reason="
            + error.getReason()
            + ", location="
            + error.getLocation());
      } else {
        log.info("Job completed successfully");
      }
    } catch (InterruptedException e) {
      throw new CircusTrainException(e);
    }
  }
}
