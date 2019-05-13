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
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlShowPlugin;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import java.io.IOException;
import java.util.Collections;

public class ShowPluginHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShowTablesHandler.class);

  public ShowPluginHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws IOException, ForemanSetupException {
    SqlShowPlugin node = unwrap(sqlNode, SqlShowPlugin.class);

    StoragePlugin plugin = null;
    try {
      plugin = context.getStorage().getPlugin(node.getName());
    } catch (ExecutionSetupException e) {
      logger.error("Failure on StoragePlugin initialization", e);
    }

    if (plugin == null) {
      return DirectPlan.createDirectPlan(context.getCurrentEndpoint(), Collections.emptyList(), CommandResult.class);
    }

    CommandResult result = new CommandResult();
    result.name = node.getName();
    result.enabled = plugin.getConfig().isEnabled();
    result.configuration = context.getLpPersistence().getMapper().writeValueAsString(plugin.getConfig());

    return DirectPlan.createDirectPlan(context.getCurrentEndpoint(), Collections.singletonList(result), CommandResult.class);
  }

  public static class CommandResult {
    public String name;
    public boolean enabled;
    public String configuration;
  }
}
