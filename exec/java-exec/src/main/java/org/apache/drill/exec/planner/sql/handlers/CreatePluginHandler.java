/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreatePlugin;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.IOException;

public class CreatePluginHandler extends DefaultSqlHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreatePluginHandler.class);

  public CreatePluginHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Creates a storage configuration
   *
   * @return - Single row indicating status of storage plugin, or error message otherwise.
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException {
    SqlCreatePlugin node = unwrap(sqlNode, SqlCreatePlugin.class);

    StoragePlugin plugin = null;
    try {
      plugin = context.getStorage().getPlugin(node.getName());
    } catch (StoragePluginRegistry.PluginException e) {
      logger.error("Failure on StoragePlugin initialization", e);
    }

    if (plugin != null) {
      String message = String.format("A plugin with given name [%s] already exists.", node.getName());

      if (node.getCreateStorageType() == SqlCreatePlugin.SqlCreateStorageType.SIMPLE) {
        throw UserException.planError()
          .message(message)
          .build(logger);
      }

      if (node.getCreateStorageType() == SqlCreatePlugin.SqlCreateStorageType.IF_NOT_EXISTS) {
        return DirectPlan.createDirectPlan(context, false, message);
      }
    }

    StoragePluginConfig config;
    try {
      config = context.getLpPersistence().getMapper().readValue(node.getConfiguration(), StoragePluginConfig.class);
    } catch (IOException e) {
      throw UserException.planError(e)
        .message("Failure while parsing plugin configuration.")
        .build(logger);
    }

    try {
      config.setEnabled(true);
      context.getStorage().putJson(node.getName(), context.getLpPersistence().getMapper().writeValueAsString(config));
    } catch (StoragePluginRegistry.PluginException | JsonProcessingException e) {
      throw UserException.planError(e)
        .message(String.format("Failure while plugin [%s] initialization", node.getName()))
        .build(logger);
    }

    boolean replaced = plugin != null;
    return DirectPlan.createDirectPlan(context, true,
      String.format("Plugin '%s' %s successfully.", node.getName(), replaced ? "replaced" : "created"));
  }
}
