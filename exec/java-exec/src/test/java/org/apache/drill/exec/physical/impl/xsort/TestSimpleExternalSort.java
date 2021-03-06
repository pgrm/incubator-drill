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
package org.apache.drill.exec.physical.impl.xsort;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestSimpleExternalSort extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleExternalSort.class);
  DrillConfig c = DrillConfig.create();


  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(80000);

  @Test
  public void sortOneKeyDescendingMergeSort() throws Throwable{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        Drillbit bit2 = new Drillbit(CONFIG, serviceSet);
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      bit2.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/xsort/one_key_sort_descending.json"),
                      Charsets.UTF_8));
      int count = 0;
      for(QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() != 0)
          count += b.getHeader().getRowCount();
      }
      assertEquals(1000000, count);

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() == 0) break;
        batchCount++;
        RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
        loader.load(b.getHeader().getDef(),b.getData());
        BigIntVector c1 = (BigIntVector) loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();


        BigIntVector.Accessor a1 = c1.getAccessor();
//        IntVector.Accessor a2 = c2.getAccessor();

        for(int i =0; i < c1.getAccessor().getValueCount(); i++){
          recordCount++;
          assertTrue(String.format("%d > %d", previousBigInt, a1.get(i)), previousBigInt >= a1.get(i));
          previousBigInt = a1.get(i);
        }
        loader.clear();
        b.release();
      }

      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));

    }
  }

  @Test
  public void sortOneKeyDescendingExternalSort() throws Throwable{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create("drill-external-sort.conf");

    try(Drillbit bit1 = new Drillbit(config, serviceSet);
        Drillbit bit2 = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {

      bit1.run();
      bit2.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/xsort/one_key_sort_descending.json"),
                      Charsets.UTF_8));
      int count = 0;
      for(QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() != 0)
          count += b.getHeader().getRowCount();
      }
      assertEquals(1000000, count);

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() == 0) break;
        batchCount++;
        RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
        loader.load(b.getHeader().getDef(),b.getData());
        BigIntVector c1 = (BigIntVector) loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();


        BigIntVector.Accessor a1 = c1.getAccessor();

        for(int i =0; i < c1.getAccessor().getValueCount(); i++){
          recordCount++;
          assertTrue(String.format("%d < %d", previousBigInt, a1.get(i)), previousBigInt >= a1.get(i));
          previousBigInt = a1.get(i);
        }
        loader.clear();
        b.release();
      }

      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));

    }
  }

  @Test
  public void outOfMemoryExternalSort() throws Throwable{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create("drill-oom-xsort.conf");

    try(Drillbit bit1 = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/xsort/oom_sort_test.json"),
                      Charsets.UTF_8));
      int count = 0;
      for(QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() != 0)
          count += b.getHeader().getRowCount();
      }
      assertEquals(10000000, count);

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryResultBatch b : results) {
        if (b.getHeader().getRowCount() == 0) break;
        batchCount++;
        RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
        loader.load(b.getHeader().getDef(),b.getData());
        BigIntVector c1 = (BigIntVector) loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();


        BigIntVector.Accessor a1 = c1.getAccessor();

        for(int i =0; i < c1.getAccessor().getValueCount(); i++){
          recordCount++;
          assertTrue(String.format("%d < %d", previousBigInt, a1.get(i)), previousBigInt >= a1.get(i));
          previousBigInt = a1.get(i);
        }
        assertTrue(String.format("%d == %d", a1.get(0), a1.get(a1.getValueCount() - 1)), a1.get(0) != a1.get(a1.getValueCount() - 1));
        loader.clear();
        b.release();
      }

      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));

    }
  }
}
