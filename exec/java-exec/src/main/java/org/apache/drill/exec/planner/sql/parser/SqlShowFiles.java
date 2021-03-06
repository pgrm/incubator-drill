/**
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

import com.google.common.collect.Lists;
import net.hydromatic.optiq.tools.Planner;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.ShowFileHandler;
import org.apache.drill.exec.planner.sql.handlers.ShowTablesHandler;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Sql parse tree node to represent statement:
 * SHOW FILES [{FROM | IN} db_name] [LIKE 'pattern' | WHERE expr]
 */
public class SqlShowFiles extends DrillSqlCall {

  private final SqlIdentifier db;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SHOW_FILES", SqlKind.OTHER);

  public SqlShowFiles(SqlParserPos pos, SqlIdentifier db) {
    super(pos);
    this.db = db;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    if (db != null) opList.add(db);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("FILES");
    if (db != null) db.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(Planner planner, QueryContext context) {
    return new ShowFileHandler(planner, context);
  }
  public SqlIdentifier getDb() { return db; }
}
