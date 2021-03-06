# Circus Train BigQuery To Hive Replication

##  Overview
This [Circus Train](https://github.com/HotelsDotCom/circus-train) plugin enables the conversion of BigQuery tables to Hive.

# Start using
You can obtain Circus Train BigQuery from Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/circus-train-bigquery/badge.svg?subject=com.hotels:circus-train-bigquery)](https://maven-badges.herokuapp.com/maven-central/com.hotels/circus-train-bigquery) [![Build Status](https://travis-ci.org/HotelsDotCom/circus-train-bigquery.svg?branch=master)](https://travis-ci.org/HotelsDotCom/circus-train-bigquery) [![Coverage Status](https://coveralls.io/repos/github/HotelsDotCom/circus-train-bigquery/badge.svg?branch=master)](https://coveralls.io/github/HotelsDotCom/circus-train-bigquery?branch=master) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/circus-train.svg)

## Configuration
* Add the `circus-train-bigquery` jar to your `CIRCUS_TRAIN_CLASSPATH`, or as a dependency on your Circus Train  project.
* Configure Circus Train as you would for a copy job from Google Cloud [Configuration](https://github.com/HotelsDotCom/circus-train/tree/master/circus-train-gcp)
* Provide the Google Cloud project ID that your BigQuery instance resides in as your `source-catalog` `hive-metastore-uris` parameter using the format `hive-metastore-uris: bigquery://<project-id>`
* To enable copying to Google Storage provide a path to your Google Credentials in the configuration under the gcp-security parameter.
* Provide your BigQuery dataset as `source-table` `database-name` and your BigQuery table name as `source-table` `table-name`


#### Example:

    source-catalog:
      name: my-google-source-catalog
      hive-metastore-uris: bigquery://my-gcp-project-id
    replica-catalog:
      name: my-replica-catalog
      hive-metastore-uris: thrift://internal-shared-hive-metastore-elb-123456789.us-west-2.elb.foobaz.com:9083
    gcp-security:
      credential-provider: /home/hadoop/.gcp/my-gcp-project-01c26fd71db7.json 

    table-replications:
    - source-table:
        database-name: mysourcedb
        table-name: google_ads_data
      replica-table:
        database-name: myreplicadb
        table-name: bigquery_google_ads_data
        table-location: s3://mybucket/foo/baz/


#### Technical Overview
The BigQuery plugin works by extracting the BigQuery table data into Google Storage using Google APIs, and then using Circus Train
listeners to convert the BigQuery metadata into a Hive table object. The data is then replicated from source to replica using 
the metadata from this mocked Hive table.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018 Expedia Inc.
