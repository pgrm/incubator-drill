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
package org.apache.drill.yarn.integration;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.server.StartupOptions;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.*;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.yarn.YarnTwillRunnerService;

public class RunDrill {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunDrill.class);

  private TwillRunnerService twillRunner;
  private TwillController controller;
  private String zkConnection;
  private DrillConfig config;

  public static void main(String[] args) {
    final RunDrill starter = new RunDrill();

    if (args.length < 1) {
      System.err.println("Arguments format: <host:port of zookeeper server>");
    }

    starter.zkConnection = args[0];

    starter.initializeDrillConfig();
    starter.initializeAndStartTwillRunnerService();
    starter.initializeAndStartTwillController();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        starter.controller.stopAndWait();
        starter.twillRunner.stopAndWait();
      }
    });

    try {
      Services.getCompletionFuture(starter.controller).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private void initializeDrillConfig() {
    StartupOptions options = StartupOptions.parse(new String[0]);
    this.config = DrillConfig.create(options.getConfigLocation());
  }

  private void initializeAndStartTwillRunnerService() {
    twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), this.zkConnection);
    twillRunner.startAndWait();
  }

  private void initializeAndStartTwillController() {
    controller = twillRunner.prepare(new DrillbitRunnable(), getResourceSpecifications())
                    .withDependencies(getStorageConfigDependencies())
                    .withDependencies(getStoragePluginDependencies())
                    .withDependencies(getFormatPluginDependencies())
                    .withDependencies(getDrillFunctionDependencies())
                    .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                    .start();

  }

  private ResourceSpecification getResourceSpecifications() {
    return ResourceSpecification.Builder.with()
            .setVirtualCores(2)
            .setMemory(2048, ResourceSpecification.SizeUnit.MEGA)
            .setInstances(1)
            .build();
  }

  private Iterable<Class<?>> getStorageConfigDependencies() {
    Set<Class<? extends StoragePluginConfig>> types;

    types = PathScanner.scanForImplementations(StoragePluginConfig.class, Lists.newArrayList("org"));
    return convertTypesToClass(types);
  }

  private Iterable<Class<?>> getStoragePluginDependencies() {
    Set<Class<? extends StoragePlugin>> types;

    types = PathScanner.scanForImplementations(StoragePlugin.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    return convertTypesToClass(types);
  }

  private Iterable<Class<?>> getFormatPluginDependencies() {
    Set<Class<? extends FormatPlugin>> types;

    types = PathScanner.scanForImplementations(FormatPlugin.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    return convertTypesToClass(types);
  }

  private Iterable<Class<?>> getDrillFunctionDependencies() {
    Set<Class<? extends DrillFunc>> types;

    types =  PathScanner.scanForImplementations(DrillFunc.class, config.getStringList(ExecConstants.FUNCTION_PACKAGES));
    return convertTypesToClass(types);
  }

/*
  private Iterable<Class<?>> getHiveFunctionDependencies() {
    Set<Class<? extends GenericUDF>> types;

    types =  PathScanner.scanForImplementations(GenericUDF.class, null);
    return convertTypesToClass(types);
  }
*/

  private static <T> Iterable<Class<?>> convertTypesToClass(Iterable<Class<? extends T>> types) {
    List<Class<?>> ret = new LinkedList<>();

    for (Class<?> type : types) {
      ret.add(type);
    }

    return ret;
  }
}
