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
package com.hotels.bdp.circustrain.bigquery.conversion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;

public class BigQueryToHivePartitionConverter {

  private Partition partition = new Partition();

  public BigQueryToHivePartitionConverter() {
    partition.setDbName("default");
    partition.setTableName("default");
    partition.setValues(new ArrayList<String>());
    partition.setLastAccessTime(0);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("");
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<String>());
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setParameters(new HashMap<String, String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setCompressed(false);
    sd.setStoredAsSubDirectories(false);
    sd.setNumBuckets(-1);
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    Map<String, String> serDeParameters = new HashMap<>();
    serDeParameters.put("serialization.format", ",");
    serDeParameters.put("field.delim", ",");
    serDeInfo.setParameters(serDeParameters);
    sd.setSerdeInfo(serDeInfo);
    partition.setSd(sd);
  }

  public Partition convert() {
    return new Partition(partition);
  }

  public BigQueryToHivePartitionConverter withDatabaseName(String dbName) {
    partition.setDbName(dbName);
    return this;
  }

  public BigQueryToHivePartitionConverter withTableName(String tableName) {
    partition.setTableName(tableName);
    return this;
  }

  public BigQueryToHivePartitionConverter withLocation(String location) {
    partition.getSd().setLocation(location);
    return this;
  }

  // TODO: Refactor so that Fields and FieldValueList are passed rather than TableResult
  public BigQueryToHivePartitionConverter withValues(TableResult result) {
    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    // NOTE: Should this be set or list? Does Hive take duplicate Partitions
    Set<String> values = new LinkedHashSet<>();
    Schema schema = result.getSchema();
    for (Field field : schema.getFields()) {
      String key = field.getName().toLowerCase();
      for (FieldValueList row : result.iterateAll()) {
        String data = row.get(key).getValue().toString();
        String value = key + "=" + data;
        values.add(value);
      }
    }

    for (String value : values) {
      partition.addToValues(value);
    }
    return this;
  }
}
