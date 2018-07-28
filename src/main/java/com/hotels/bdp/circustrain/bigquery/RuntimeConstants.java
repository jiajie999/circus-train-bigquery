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
package com.hotels.bdp.circustrain.bigquery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RuntimeConstants {

  public static int NUM_THREADS;

  private static final Logger log = LoggerFactory.getLogger(RuntimeConstants.class);

  static {
    NUM_THREADS = Runtime.getRuntime().availableProcessors() <= 0 ? 2 : Runtime.getRuntime().availableProcessors() + 1;
    log.debug("RuntimeConstants default number of threads set to {}", NUM_THREADS);
  }

}
