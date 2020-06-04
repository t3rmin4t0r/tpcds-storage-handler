/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.notmysock.benchmark.tpcds;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.notmysock.benchmark.tpcds.TpcdsTableProperties.TABLE_NAME;


/**
 * TPCDS storage handler to allow user querying Stream of tuples from a table.
 */
public class TpcdsHandler implements HiveStorageHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(TpcdsHandler.class);

  Configuration configuration;
  
  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return TpcdsInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return NullOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return TextTpcdsSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider()
      throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }
  
  private void configureCommonProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    Properties props = tableDesc.getProperties();
    String table = props.getProperty(
        TABLE_NAME.getKey(), tableDesc.getTableName());
    
    jobProperties.put(TABLE_NAME.getKey(), table);
   
    for (TpcdsTableProperties option : TpcdsTableProperties.getOptions()) {
      String value = props.getProperty(option.getKey(), option.getDefaultValue());
      jobProperties.put(option.getKey(), value);
    }
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureCommonProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureCommonProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureInputJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureInputJobCredentials(TableDesc tableDesc,
      Map<String, String> secrets) {
    // no secrets
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    Map<String, String> properties = new HashMap<>();
    configureInputJobProperties(tableDesc, properties);
    properties.forEach((key, value) -> jobConf.set(key, value));
    try {
      TpcdsUtils.copyDependencyJars(jobConf, this.getClass());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
