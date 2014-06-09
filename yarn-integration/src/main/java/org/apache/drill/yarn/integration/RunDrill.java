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
import java.util.concurrent.ExecutionException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.StartupOptions;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.*;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.yarn.YarnTwillRunnerService;
import parquet.format.converter.ParquetMetadataConverter;

public class RunDrill {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunDrill.class);

  /**
   * BundledJarApplication that specifies a single instance of main.sample.Scratch
   * to be run from a bundled jar.
   */
  private static class DrillbitRunnable extends AbstractTwillRunnable {
    @Override
    public void run() {
      StartupOptions options = StartupOptions.parse(super.getContext().getArguments());
      DrillConfig config = DrillConfig.create(options.getConfigLocation());

      try {
        this.start(config);
      } catch (DrillbitStartupException e) {
        logger.error("Error starting Drillbit.", e);
      }
    }

    private Drillbit start(DrillConfig config) throws DrillbitStartupException {
      Drillbit bit;
      try {
        logger.debug("Setting up Drillbit.");
        bit = new Drillbit(config, null);
      } catch (Exception ex) {
        throw new DrillbitStartupException("Failure while initializing values in Drillbit.", ex);
      }
      try {
        logger.debug("Starting Drillbit.");
        bit.run();
      } catch (Exception e) {
        throw new DrillbitStartupException("Failure during initial startup of Drillbit.", e);
      }
      return bit;
    }
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Arguments format: <host:port of zookeeper server>");
    }

    String zkStr = args[0];

    final TwillRunnerService twillRunner =
            new YarnTwillRunnerService(
                    new YarnConfiguration(), zkStr);
    twillRunner.startAndWait();

    final TwillController controller =
            twillRunner.prepare(new DrillbitRunnable(),
                    ResourceSpecification.Builder.with()
                            .setVirtualCores(2)
                            .setMemory(2048, ResourceSpecification.SizeUnit.MEGA)
                            .setInstances(1)
                            .build())
                    .withDependencies(ParquetMetadataConverter.class)
                    .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                    .start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        controller.stopAndWait();
        twillRunner.stopAndWait();
      }
    });

    try {
      Services.getCompletionFuture(controller).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }
}
