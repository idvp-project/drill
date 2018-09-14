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

import org.apache.calcite.sql.SqlNode;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateStorage;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.IOException;

public class CreateStorageHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateStorageHandler.class);

  public CreateStorageHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Creates a storage configuration
   *
   * @return - Single row indicating status of storage plugin, or error message otherwise.
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException {
    SqlCreateStorage node = unwrap(sqlNode, SqlCreateStorage.class);

    StoragePlugin plugin = null;
    try {
      plugin = context.getStorage().getPlugin(node.getName());
    } catch (ExecutionSetupException e) {
      logger.error("Failure on StoragePlugin initialization", e);
    }

    if (plugin != null) {
      String message = String.format("A storage with given name [%s] already exists.", node.getName());

      if (node.getCreateStorageType() == SqlCreateStorage.SqlCreateStorageType.SIMPLE) {
        throw UserException.planError()
          .message(message)
          .build(logger);
      }

      if (node.getCreateStorageType() == SqlCreateStorage.SqlCreateStorageType.IF_NOT_EXISTS) {
        return DirectPlan.createDirectPlan(context, false, message);
      }
    }

    StoragePluginConfig config;
    try {
      config = context.getLpPersistence().getMapper().readValue(node.getConfiguration(), StoragePluginConfig.class);
    } catch (IOException e) {
      throw UserException.planError(e)
        .message("Failure while parsing storage configuration.")
        .build(logger);
    }

    try {
      context.getStorage().createOrUpdate(node.getName(), config, true);
    } catch (ExecutionSetupException e) {
      throw UserException.planError(e)
        .message(String.format("Failure while storage [%s] initialization", node.getName()))
        .build(logger);
    }

    boolean replaced = plugin != null;
    return DirectPlan.createDirectPlan(context, true,
      String.format("Storage '%s' %s successfully.", node.getName(), replaced ? "replaced" : "created"));
  }
}
