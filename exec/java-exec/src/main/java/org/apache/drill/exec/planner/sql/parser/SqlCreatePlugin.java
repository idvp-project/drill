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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.CreatePluginHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.List;

/**
 * Sql parse tree node to represent statement:
 * CREATE ([OR REPLACE] PLUGIN | PLUGIN [IF NOT EXISTS]) USING 'config'
 */
public class SqlCreatePlugin extends DrillSqlCall {

  private final SqlIdentifier storageName;
  private final SqlLiteral createStorageType;
  private final SqlNode configuration;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_STORAGE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlCreatePlugin(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1], operands[0]);
    }
  };

  public SqlCreatePlugin(SqlParserPos pos, SqlIdentifier storageName, SqlLiteral createStorageType, SqlNode configuration) {
    super(pos);
    this.storageName = storageName;
    this.createStorageType = createStorageType;
    this.configuration = configuration;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(storageName, createStorageType, configuration);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    switch (SqlCreateStorageType.valueOf(createStorageType.toValue())) {
      case SIMPLE:
        writer.keyword("PLUGIN");
        break;
      case OR_REPLACE:
        writer.keyword("OR");
        writer.keyword("REPLACE");
        writer.keyword("PLUGIN");
        break;
      case IF_NOT_EXISTS:
        writer.keyword("PLUGIN");
        writer.keyword("IF");
        writer.keyword("NOT");
        writer.keyword("EXISTS");
        break;
    }
    storageName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("USING");
    configuration.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new CreatePluginHandler(config);
  }

  public String getName() {
    if (storageName.isSimple()) {
      return storageName.getSimple();
    }

    return storageName.names.get(storageName.names.size() - 1);
  }

  public SqlCreateStorageType getCreateStorageType() {
    return SqlCreateStorageType.valueOf(createStorageType.toValue());
  }

  public String getConfiguration() {
    return ((SqlCharStringLiteral) configuration).toValue();
  }

  public enum SqlCreateStorageType {
    SIMPLE, OR_REPLACE, IF_NOT_EXISTS
  }
}
