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
package org.apache.drill.exec.expr;

import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.Decimal18Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal28Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal38Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal9Expression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalDayExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalYearExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.ConstantExpressionIdentifier;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.HiveFuncHolder;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.record.NullExpression;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

import com.google.common.collect.Lists;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JLabel;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public class EvaluationVisitor {

  private final FunctionImplementationRegistry registry;

  public EvaluationVisitor(FunctionImplementationRegistry registry) {
    super();
    this.registry = registry;
  }

  public HoldingContainer addExpr(LogicalExpression e, ClassGenerator<?> generator){
    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(e);
    //Set<LogicalExpression> constantBoundaries = Collections.emptySet();
    return e.accept(new ConstantFilter(constantBoundaries), generator);
  }

  private class EvalVisitor extends AbstractExprVisitor<HoldingContainer, ClassGenerator<?>, RuntimeException> {


    @Override
    public HoldingContainer visitFunctionCall(FunctionCall call, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("FunctionCall is not expected here. "+
        "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(
      FunctionHolderExpression holderExpr, ClassGenerator<?> generator) throws RuntimeException {
      // TODO: hack: (Drill/Hive)FuncHolderExpr reference classes in exec so
      // code generate methods can't be superclass FunctionHolderExpression
      // which is defined in common

      if (holderExpr instanceof DrillFuncHolderExpr) {
        DrillFuncHolder holder = ((DrillFuncHolderExpr)holderExpr).getHolder();
        JVar[] workspaceVars = holder.renderStart(generator, null);

        if(holder.isNested()) generator.getMappingSet().enterChild();

        HoldingContainer[] args = new HoldingContainer[holderExpr.args.size()];
        for (int i = 0; i < holderExpr.args.size(); i++) {
          args[i] = holderExpr.args.get(i).accept(this, generator);
        }

        holder.renderMiddle(generator, args, workspaceVars);

        if(holder.isNested()) generator.getMappingSet().exitChild();

        return holder.renderEnd(generator, args, workspaceVars);

      } else if (holderExpr instanceof HiveFuncHolderExpr) {

        HiveFuncHolder holder = ((HiveFuncHolderExpr)holderExpr).getHolder();

        HoldingContainer[] args = new HoldingContainer[holderExpr.args.size()];
        for (int i = 0; i < holderExpr.args.size(); i++) {
          args[i] = holderExpr.args.get(i).accept(this, generator);
        }

        return holder.renderEnd(generator, args, holder.renderStart(generator, null));
      }

      throw new UnsupportedOperationException(String.format("Unknown expression '%s'", holderExpr.getClass().getCanonicalName()));
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression ifExpr, ClassGenerator<?> generator) throws RuntimeException {
      JBlock local = generator.getEvalBlock();

      HoldingContainer output = generator.declare(ifExpr.getMajorType());

      JConditional jc = null;
      JBlock conditionalBlock = new JBlock(false, false);
      for (IfCondition c : ifExpr.conditions) {
        HoldingContainer holdingContainer = c.condition.accept(this, generator);
        if (jc == null) {
          if (holdingContainer.isOptional()) {
            jc = conditionalBlock._if(holdingContainer.getIsSet().cand(holdingContainer.getValue()));
          } else {
            jc = conditionalBlock._if(holdingContainer.getValue().eq(JExpr.lit(1)));
          }
        } else {
          if (holdingContainer.isOptional()) {
            jc = jc._else()._if(holdingContainer.getIsSet().cand(holdingContainer.getValue()));
          } else {
            jc = jc._else()._if(holdingContainer.getValue());
          }
        }

        HoldingContainer thenExpr = c.expression.accept(this, generator);
        if (thenExpr.isOptional()) {
          JConditional newCond = jc._then()._if(thenExpr.getIsSet());
          JBlock b = newCond._then();
          b.assign(output.getHolder(), thenExpr.getHolder());
          b.assign(output.getIsSet(), thenExpr.getIsSet());
        } else {
          jc._then().assign(output.getHolder(), thenExpr.getHolder());
        }

      }

      HoldingContainer elseExpr = ifExpr.elseExpression.accept(this, generator);
      if (elseExpr.isOptional()) {
        JConditional newCond = jc._else()._if(elseExpr.getIsSet());
        JBlock b = newCond._then();
        b.assign(output.getHolder(), elseExpr.getHolder());
        b.assign(output.getIsSet(), elseExpr.getIsSet());
      } else {
        jc._else().assign(output.getHolder(), elseExpr.getHolder());
      }
      local.add(conditionalBlock);
      return output;
    }


    @Override
    public HoldingContainer visitSchemaPath(SchemaPath path, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("All schema paths should have been replaced with ValueVectorExpressions.");
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getLong()));
      return out;
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getInt()));
      return out;
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getDate()));
      return out;
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getTime()));
      return out;
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getIntervalYear()));
      return out;
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getTimeStamp()));
      return out;
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getDouble()));
      return out;
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getBoolean() ? 1 : 0));
      return out;
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (e instanceof ValueVectorReadExpression) {
        return visitValueVectorReadExpression((ValueVectorReadExpression) e, generator);
      } else if (e instanceof ValueVectorWriteExpression) {
        return visitValueVectorWriteExpression((ValueVectorWriteExpression) e, generator);
      } else if (e instanceof ReturnValueExpression) {
        return visitReturnValueExpression((ReturnValueExpression) e, generator);
      }else if(e instanceof HoldingContainerExpression){
        return ((HoldingContainerExpression) e).getContainer();
      }else if(e instanceof NullExpression){
        return generator.declare(Types.optional(MinorType.INT));
      } else if (e instanceof TypedNullConstant) {
        return generator.declare(e.getMajorType());
      }    else {
        return super.visitUnknown(e, generator);
      }

    }

    private HoldingContainer visitValueVectorWriteExpression(ValueVectorWriteExpression e, ClassGenerator<?> generator) {

      final LogicalExpression child = e.getChild();
      final HoldingContainer inputContainer = child.accept(this, generator);
      final boolean complex = Types.isComplex(inputContainer.getMajorType());

      JBlock block = generator.getEvalBlock();
      JExpression outIndex = generator.getMappingSet().getValueWriteIndex();
      JVar vv = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getOutgoing(), e.getFieldId());

      if(complex){
        JType writerImpl = generator.getModel()._ref(TypeHelper.getWriterImpl(inputContainer.getMinorType(), inputContainer.getMajorType().getMode()));
        JType writerIFace = generator.getModel()._ref(TypeHelper.getWriterInterface(inputContainer.getMinorType(), inputContainer.getMajorType().getMode()));
        JVar writer = generator.declareClassField("writer", writerIFace);
        generator.getSetupBlock().assign(writer, JExpr._new(writerImpl).arg(vv).arg(JExpr._null()));
        generator.getEvalBlock().add(writer.invoke("setPosition").arg(outIndex));
        String copyMethod = inputContainer.isSingularRepeated() ? "copyAsValueSingle" : "copyAsValue";
        generator.getEvalBlock().add(inputContainer.getHolder().invoke(copyMethod).arg(writer));
        if(e.isSafe()){
          HoldingContainer outputContainer = generator.declare(Types.REQUIRED_BIT);
          JConditional ifOut = generator.getEvalBlock()._if(writer.invoke("ok"));
          ifOut._then().assign(outputContainer.getValue(), JExpr.lit(1));
          ifOut._else().assign(outputContainer.getValue(), JExpr.lit(0));
          return outputContainer;
        }
      }else{
        String setMethod = e.isSafe() ? "setSafe" : "set";

        String isSafeMethod = "isSafe";

        JInvocation setMeth;
        if (Types.usesHolderForGet(inputContainer.getMajorType())) {
          setMeth = vv.invoke("getMutator").invoke(setMethod).arg(outIndex).arg(inputContainer.getHolder());
        }else{
          setMeth = vv.invoke("getMutator").invoke(setMethod).arg(outIndex).arg(inputContainer.getValue());
        }

        if(e.isSafe()){
          HoldingContainer outputContainer = generator.declare(Types.REQUIRED_BIT);
          block.assign(outputContainer.getValue(), JExpr.lit(1));
          if(inputContainer.isOptional()){
//            block._if(vv.invoke("getMutator").invoke(setMethod).arg(outIndex).not())._then().assign(outputContainer.getValue(), JExpr.lit(0));
            JConditional jc = block._if(inputContainer.getIsSet().eq(JExpr.lit(0)).not());
            block = jc._then();
            jc._else()._if(vv.invoke("getMutator").invoke(isSafeMethod).arg(outIndex).not())._then().assign(outputContainer.getValue(), JExpr.lit(0));
          }
          block._if(setMeth.not())._then().assign(outputContainer.getValue(), JExpr.lit(0));
          return outputContainer;
        }else{
          if (inputContainer.isOptional()) {
//            block.add(vv.invoke("getMutator").invoke(setMethod).arg(outIndex));
            JConditional jc = block._if(inputContainer.getIsSet().eq(JExpr.lit(0)).not());
            block = jc._then();
          }
          block.add(setMeth);
        }

      }


      return null;
    }

    private HoldingContainer visitValueVectorReadExpression(ValueVectorReadExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      // declare value vector

      JExpression vv1 = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getIncoming(), e.getFieldId());
      JExpression indexVariable = generator.getMappingSet().getValueReadIndex();

      JExpression componentVariable = indexVariable.shrz(JExpr.lit(16));
      if (e.isSuperReader()) {
        vv1 =  (vv1.component(componentVariable));
        indexVariable = indexVariable.band(JExpr.lit((int) Character.MAX_VALUE));
      }

      // evaluation work.
      HoldingContainer out = generator.declare(e.getMajorType());
      final boolean primitive = !Types.usesHolderForGet(e.getMajorType());
      final boolean hasReadPath = e.hasReadPath();
      final boolean complex = Types.isComplex(e.getMajorType());

      int[] fieldIds = e.getFieldId().getFieldIds();
      for(int i = 1; i < fieldIds.length; i++){

      }

      if(!hasReadPath && !complex){

        JInvocation getValueAccessor = vv1.invoke("getAccessor").invoke("get");
        JInvocation getValueAccessor2 = vv1.invoke("getAccessor");
        JBlock eval = new JBlock();

        if(primitive){
          eval.assign(out.getValue(), getValueAccessor.arg(indexVariable));
        }else{
          eval.add(getValueAccessor.arg(indexVariable).arg(out.getHolder()));
        }

        if (out.isOptional()) {
          JBlock blk = generator.getEvalBlock();
          blk.assign(out.getIsSet(), getValueAccessor2.invoke("isSet").arg(indexVariable));
          JConditional jc = blk._if(out.getIsSet().eq(JExpr.lit(1)));
          jc._then().add(eval);
        }else{
          generator.getEvalBlock().add(eval);
        }

      }else{
        JExpression vector = e.isSuperReader() ? vv1.component(componentVariable) : vv1;
        JExpression expr = vector.invoke("getAccessor").invoke("getReader");

        JLabel label = generator.getEvalBlock().label("complex");
        JBlock eval = generator.getEvalBlock().block();

        // position to the correct value.
        eval.add(expr.invoke("setPosition").arg(indexVariable));
        PathSegment seg = e.getReadPath();
        int listNum = 0;
        boolean lastWasArray = false;

        while(seg != null){
          if(seg.isArray()){
            lastWasArray = true;

            if(seg.isLastPath() && !complex) break;

            JVar list = generator.declareClassField("list", generator.getModel()._ref(FieldReader.class));
            generator.getSetupBlock().assign(list, expr);
            expr = list;

            // if this is an array, set a single position for the expression to allow us to read the right data lower down.
            JVar desiredIndex = eval.decl(generator.getModel().INT, "desiredIndex" + listNum, JExpr.lit(seg.getArraySegment().getIndex()));
            // start with negative one so that we are at zero after first call to next.
            JVar currentIndex = eval.decl(generator.getModel().INT, "currentIndex" + listNum, JExpr.lit(-1));

            eval._while( //
                currentIndex.lt(desiredIndex) //
                .cand(expr.invoke("next")) ).body().assign(currentIndex, currentIndex.plus(JExpr.lit(1)));

            JBlock ifNoVal = eval._if(desiredIndex.ne(currentIndex))._then().block();
            if(!complex) ifNoVal.assign(out.getIsSet(), JExpr.lit(0));
            ifNoVal._break(label);

            listNum++;

          }else{
            lastWasArray = false;
            JExpression fieldName = JExpr.lit(seg.getNameSegment().getPath());
            expr = expr.invoke("reader").arg(fieldName);
          }
          seg = seg.getChild();

          // stop once we get to last column or when the segment is an array at the end of the reference.
          if(seg == null || seg.isLastPath() && seg.isArray()) break;
        }
        MajorType secondaryType = e.getFieldId().getSecondaryFinal();
        JType readerImpl = generator.getModel()._ref(TypeHelper.getReaderClassName(secondaryType.getMinorType(), secondaryType.getMode()));
        JVar complexReader = generator.declareClassField("reader", readerImpl);
        generator.getSetupBlock().assign(complexReader, JExpr.cast(readerImpl, expr));
        expr = complexReader;

        if(complex){
          HoldingContainer hc = new HoldingContainer(e.getMajorType(), (JVar) expr, null, null, lastWasArray);
          return hc;
          //eval.assign(out.getHolder().ref("reader"), expr);
        }else{
          if(seg != null){
            eval.add(expr.invoke("read").arg(JExpr.lit(seg.getArraySegment().getIndex())).arg(out.getHolder()));
          }else{

            eval.add(expr.invoke("read").arg(out.getHolder()));
          }
        }

      }




      return out;
    }

    private HoldingContainer visitReturnValueExpression(ReturnValueExpression e, ClassGenerator<?> generator) {
      LogicalExpression child = e.getChild();
      // Preconditions.checkArgument(child.getMajorType().equals(Types.REQUIRED_BOOLEAN));
      HoldingContainer hc = child.accept(this, generator);
      if(e.isReturnTrueOnOne()){
        generator.getEvalBlock()._return(hc.getValue().eq(JExpr.lit(1)));
      }else{
        generator.getEvalBlock()._return(hc.getValue());
      }

      return null;
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = Types.required(MinorType.VARCHAR);
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("string", holderType);
      JExpression stringLiteral = JExpr.lit(e.value);
      setup.assign(var, generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getVarCharHolder").arg(stringLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = Types.required(MinorType.INTERVALDAY);
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("intervalday", holderType);
      JExpression dayLiteral = JExpr.lit(e.getIntervalDay());
      JExpression millisLiteral = JExpr.lit(e.getIntervalMillis());
      setup.assign(var, generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getIntervalDayHolder").arg(dayLiteral).arg(millisLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal9Constant(Decimal9Expression e, ClassGenerator<?> generator) throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec9", holderType);
      JExpression valueLiteral = JExpr.lit(e.getIntFromDecimal());
      JExpression scaleLiteral = JExpr.lit(e.getScale());
      JExpression precisionLiteral = JExpr.lit(e.getPrecision());
      setup.assign(var, generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getDecimal9Holder").arg(valueLiteral).arg(scaleLiteral).arg(precisionLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal18Constant(Decimal18Expression e, ClassGenerator<?> generator) throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec18", holderType);
      JExpression valueLiteral = JExpr.lit(e.getLongFromDecimal());
      JExpression scaleLiteral = JExpr.lit(e.getScale());
      JExpression precisionLiteral = JExpr.lit(e.getPrecision());
      setup.assign(var, generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getDecimal18Holder").arg(valueLiteral).arg(scaleLiteral).arg(precisionLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal28Constant(Decimal28Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec28", holderType);
      JExpression stringLiteral = JExpr.lit(e.getBigDecimal().toString());
      setup.assign(var, generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getDecimal28Holder").arg(stringLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal38Constant(Decimal38Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec38", holderType);
      JExpression stringLiteral = JExpr.lit(e.getBigDecimal().toString());
      setup.assign(var, generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getVarCharHolder").arg(stringLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitCastExpression(CastExpression e, ClassGenerator<?> value) throws RuntimeException {
      throw new UnsupportedOperationException("CastExpression is not expected here. "+
        "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitConvertExpression(ConvertExpression e, ClassGenerator<?> value) throws RuntimeException {
      String convertFunctionName = e.getConvertFunction() + e.getEncodingType();

      List<LogicalExpression> newArgs = Lists.newArrayList();
      newArgs.add(e.getInput());  //input_expr

      FunctionCall fc = new FunctionCall(convertFunctionName, newArgs, e.getPosition());
      return fc.accept(this, value);
    }
  }


  private class ConstantFilter extends EvalVisitor {

    private Set<LogicalExpression> constantBoundaries;


    public ConstantFilter(Set<LogicalExpression> constantBoundaries) {
      super();
      this.constantBoundaries = constantBoundaries;
    }

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall e, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("FunctionCall is not expected here. "+
        "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitFunctionHolderExpression(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitFunctionHolderExpression(e, generator).setConstant(true);
      } else {
        return super.visitFunctionHolderExpression(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIfExpression(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIfExpression(e, generator).setConstant(true);
      } else {
        return super.visitIfExpression(e, generator);
      }
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitSchemaPath(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitSchemaPath(e, generator).setConstant(true);
      } else {
        return super.visitSchemaPath(e, generator);
      }
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitLongConstant(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitLongConstant(e, generator).setConstant(true);
      } else {
        return super.visitLongConstant(e, generator);
      }
    }


    @Override
    public HoldingContainer visitDecimal9Constant(Decimal9Expression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal9Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal9Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal9Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimal18Constant(Decimal18Expression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal18Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal18Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal18Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimal28Constant(Decimal28Expression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal28Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal28Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal28Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimal38Constant(Decimal38Expression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal38Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal38Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal38Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntConstant(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDateConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDateConstant(e, generator).setConstant(true);
      } else {
        return super.visitDateConstant(e, generator);
      }
    }


    @Override
    public HoldingContainer visitTimeConstant(TimeExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitTimeConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitTimeConstant(e, generator).setConstant(true);
      } else {
        return super.visitTimeConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntervalYearConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntervalYearConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntervalYearConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitTimeStampConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitTimeStampConstant(e, generator).setConstant(true);
      } else {
        return super.visitTimeStampConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDoubleConstant(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDoubleConstant(e, generator).setConstant(true);
      } else {
        return super.visitDoubleConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitBooleanConstant(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitBooleanConstant(e, generator).setConstant(true);
      } else {
        return super.visitBooleanConstant(e, generator);
      }
    }


    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitUnknown(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitUnknown(e, generator).setConstant(true);
      } else {
        return super.visitUnknown(e, generator);
      }
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitQuotedStringConstant(e, generator);
        //generator.getMappingSet().exitConstant();
        //return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitQuotedStringConstant(e, generator).setConstant(true);
      } else {
        return super.visitQuotedStringConstant(e, generator);
      }
    }


    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntervalDayConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntervalDayConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntervalDayConstant(e, generator);
      }
    }

    /* Get a HoldingContainer for a constant expression. The returned HoldingContainder will indicate it's for
     * a constant expression.
     * */
    private HoldingContainer renderConstantExpression(ClassGenerator<?> generator, HoldingContainer input){
      JVar fieldValue = generator.declareClassField("constant", generator.getHolderType(input.getMajorType()));
      generator.getEvalBlock().assign(fieldValue, input.getHolder());
      generator.getMappingSet().exitConstant();
      return new HoldingContainer(input.getMajorType(), fieldValue, fieldValue.ref("value"), fieldValue.ref("isSet")).setConstant(true);
    }
  }
}
