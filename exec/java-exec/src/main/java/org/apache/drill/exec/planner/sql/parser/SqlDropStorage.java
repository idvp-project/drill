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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DropStorageHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.List;

public class SqlDropStorage extends DrillSqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_STORAGE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlDropStorage(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1]);
    }
  };

  private SqlIdentifier storageName;
  private boolean storageExistenceCheck;

  public SqlDropStorage(SqlParserPos pos, SqlIdentifier storageName, SqlLiteral storageExistenceCheck) {
    this(pos, storageName, storageExistenceCheck.booleanValue());
  }

  public SqlDropStorage(SqlParserPos pos, SqlIdentifier storageName, boolean storageExistenceCheck) {
    super(pos);
    this.storageName = storageName;
    this.storageExistenceCheck = storageExistenceCheck;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    final List<SqlNode> ops =
        ImmutableList.of(
                storageName,
            SqlLiteral.createBoolean(storageExistenceCheck, SqlParserPos.ZERO)
        );
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("STORAGE");
    if (storageExistenceCheck) {
      writer.keyword("IF");
      writer.keyword("EXISTS");
    }
    storageName.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new DropStorageHandler(config);
  }

  public String getName() {
    if (storageName.isSimple()) {
      return storageName.getSimple();
    }

    return storageName.names.get(storageName.names.size() - 1);
  }

  public boolean isStorageExistenceCheck() {
    return storageExistenceCheck;
  }
}
