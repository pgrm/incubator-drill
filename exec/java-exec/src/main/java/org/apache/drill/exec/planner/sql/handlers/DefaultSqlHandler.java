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
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.common.logical.PlanProperties.PlanPropertiesBuilder;
import org.apache.drill.common.logical.PlanProperties.PlanType;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.explain.PrelSequencer;
import org.apache.drill.exec.planner.physical.visitor.FinalColumnReorderer;
import org.apache.drill.exec.planner.physical.visitor.JoinPrelRenameVisitor;
import org.apache.drill.exec.planner.physical.visitor.SelectionVectorPrelVisitor;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.util.Pointer;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.SqlNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DefaultSqlHandler extends AbstractSqlHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultSqlHandler.class);

  protected final Planner planner;
  protected final QueryContext context;
  private Pointer<String> textPlan;

  public DefaultSqlHandler(Planner planner, QueryContext context) {
    this(planner, context, null);
  }

  public DefaultSqlHandler(Planner planner, QueryContext context, Pointer<String> textPlan) {
    super();
    this.planner = planner;
    this.context = context;
    this.textPlan = textPlan;
  }

  protected void log(String name, RelNode node) {
    if (logger.isDebugEnabled()) {
      logger.debug(name + " : \n" + RelOptUtil.toString(node, SqlExplainLevel.ALL_ATTRIBUTES));
    }
  }

  protected void log(String name, Prel node) {
    String plan = PrelSequencer.printWithIds(node, SqlExplainLevel.ALL_ATTRIBUTES);;
    if(textPlan != null){
      textPlan.value = plan;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(name + " : \n" + plan);
    }
  }

  protected void log(String name, PhysicalPlan plan) throws JsonProcessingException {
    if (logger.isDebugEnabled()) {
      String planText = plan.unparse(context.getConfig().getMapper().writer());
      logger.debug(name + " : \n" + planText);
    }
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {

    SqlNode rewrittenSqlNode = rewrite(sqlNode);
    SqlNode validated = validateNode(rewrittenSqlNode);
    RelNode rel = convertToRel(validated);
    log("Optiq Logical", rel);
    DrillRel drel = convertToDrel(rel);
    log("Drill Logical", drel);
    Prel prel = convertToPrel(drel);
    log("Drill Physical", prel);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan);

    return plan;
  }

  protected SqlNode validateNode(SqlNode sqlNode) throws ValidationException, RelConversionException {
    return planner.validate(sqlNode);
  }

  protected RelNode convertToRel(SqlNode node) throws RelConversionException {
    return planner.convert(node);
  }

  protected DrillRel convertToDrel(RelNode relNode) throws RelConversionException {
    RelNode convertedRelNode = planner.transform(DrillSqlWorker.LOGICAL_RULES,
        relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);
    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    } else {
      return new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
    }
  }

  protected Prel convertToPrel(RelNode drel) throws RelConversionException {
    Preconditions.checkArgument(drel.getConvention() == DrillRel.DRILL_LOGICAL);
    RelTraitSet traits = drel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    Prel phyRelNode = (Prel) planner.transform(DrillSqlWorker.PHYSICAL_MEM_RULES, traits, drel);

    // Join might cause naming conflicts from its left and right child.
    // In such case, we have to insert Project to rename the conflicting names.
    phyRelNode = JoinPrelRenameVisitor.insertRenameProject(phyRelNode);

    // Since our operators work via names rather than indices, we have to make to reorder any output
    // before we return data to the user as we may have accindentally shuffled things.  This adds
    // a trivial project to reorder columns prior to output.
    phyRelNode = FinalColumnReorderer.addFinalColumnOrdering(phyRelNode);

    // the last thing we do is add any required selection vector removers given the supported encodings of each
    // operator. This will ultimately move to a new trait but we're managing here for now to avoid introducing new
    // issues in planning before the next release
    return SelectionVectorPrelVisitor.addSelectionRemoversWhereNecessary(phyRelNode);
  }

  protected PhysicalOperator convertToPop(Prel prel) throws IOException {
    PhysicalPlanCreator creator = new PhysicalPlanCreator(context, PrelSequencer.getIdMap(prel));
    PhysicalOperator op = prel.getPhysicalOperator(creator);
    return op;
  }

  protected PhysicalPlan convertToPlan(PhysicalOperator op) {
    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.APACHE_DRILL_PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.options(new JSONOptions(context.getOptions().getSessionOptionList()));
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator(this.getClass().getSimpleName(), "");
    return new PhysicalPlan(propsBuilder.build(), getPops(op));
  }

  public static List<PhysicalOperator> getPops(PhysicalOperator root) {
    List<PhysicalOperator> ops = Lists.newArrayList();
    PopCollector c = new PopCollector();
    root.accept(c, ops);
    return ops;
  }

  private static class PopCollector extends
      AbstractPhysicalVisitor<Void, Collection<PhysicalOperator>, RuntimeException> {

    @Override
    public Void visitOp(PhysicalOperator op, Collection<PhysicalOperator> collection) throws RuntimeException {
      collection.add(op);
      for (PhysicalOperator o : op) {
        o.accept(this, collection);
      }
      return null;
    }

  }

  /**
   * Rewrite the parse tree. Used before validating the parse tree. Useful if a particular statement needs to converted
   * into another statement.
   *
   * @param node
   * @return Rewritten sql parse tree
   * @throws RelConversionException
   */
  public SqlNode rewrite(SqlNode node) throws RelConversionException {
    return node;
  }
}
