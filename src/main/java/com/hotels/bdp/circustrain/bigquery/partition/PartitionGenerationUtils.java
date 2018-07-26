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
package com.hotels.bdp.circustrain.bigquery.partition;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.UUID;

import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;

final class PartitionGenerationUtils {

  static String randomTableName() {
    return UUID.randomUUID().toString().replaceAll("-", "_");
  }

  static boolean shouldPartition(CircusTrainBigQueryConfiguration configuration) {
    return isNotBlank(getPartitionBy(configuration));
  }

  static String getPartitionFilter(CircusTrainBigQueryConfiguration configuration) {
    if (configuration == null || configuration.getPartitionFilter() == null) {
      return null;
    }
    return configuration.getPartitionFilter().trim().toLowerCase();
  }

  static String getPartitionBy(CircusTrainBigQueryConfiguration configuration) {
    if (configuration == null || configuration.getPartitionBy() == null) {
      return null;
    }
    return configuration.getPartitionBy().trim().toLowerCase();
  }

}