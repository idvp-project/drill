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
package org.apache.drill.exec.planner.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.ShowStorageHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.Collections;
import java.util.List;

/**
 * Sql parse tree node to represent statement:
 * SHOW STORAGE name
 */
public class SqlShowStorage extends DrillSqlCall {

  private final SqlIdentifier name;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW_STORAGE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlShowStorage(pos, (SqlIdentifier) operands[0]);
    }
  };

  public SqlShowStorage(SqlParserPos pos, SqlIdentifier name) {
    super(pos);
    this.name = name;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList(name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("STORAGE");
    if (name != null) {
      name.unparse(writer, leftPrec, rightPrec);
    }
  }

  public String getName() {
    if (name.isSimple()) {
      return name.getSimple();
    }

    return name.names.get(name.names.size() - 1);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new ShowStorageHandler(config);
  }
}
