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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.StartupOptions;
import org.apache.twill.api.AbstractTwillRunnable;

public class DrillbitRunnable extends AbstractTwillRunnable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillbitRunnable.class);

  private volatile Drillbit drillbit = null;

  @Override
  public void run() {
    drillbit = initAndStart();

    while (drillbit != null) {
      try {
        synchronized (this) {
          Thread.sleep(10000);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public synchronized void stop() {
    if (drillbit != null) {
      drillbit.close();
      drillbit = null;
    }
  }

  @Override
  public void destroy() {
    stop();
  }

  private Drillbit initAndStart() {
    StartupOptions options = StartupOptions.parse(super.getContext().getArguments());
    DrillConfig config = DrillConfig.create(options.getConfigLocation());

    try {
      return this.start(config);
    } catch (DrillbitStartupException e) {
      logger.error("Error starting Drillbit.", e);
    }
    return null;
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
