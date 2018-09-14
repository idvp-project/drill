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
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlDropStorage;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

public class DropStorageHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropStorageHandler.class);

  public DropStorageHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException {
    SqlDropStorage node = unwrap(sqlNode, SqlDropStorage.class);

    StoragePlugin plugin = null;
    try {
      plugin = context.getStorage().getPlugin(node.getName());
    } catch (ExecutionSetupException e) {
      logger.error("Failure on StoragePlugin initialization", e);
    }

    if (plugin == null) {
      if (node.isStorageExistenceCheck()) {
        return DirectPlan.createDirectPlan(context, false, String.format("Storage [%s] not found.", node.getName()));
      } else {
        throw UserException.planError()
          .message(String.format("Storage [%s] not found.", node.getName()))
          .build(logger);
      }
    }

    context.getStorage().deletePlugin(node.getName());

    return DirectPlan.createDirectPlan(context, true,
      String.format("Storage '%s' deleted successfully.", node.getName()));
  }
}
