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

import org.apache.drill.exec.expr.annotations.Workspace;

<@pp.dropOutputFile />

<#list dateIntervalFunc.dates as type>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/G${type}Difference.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

import io.netty.buffer.ByteBuf;

@SuppressWarnings("unused")
@FunctionTemplate(names = {"date_diff", "subtract"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class G${type}Difference implements DrillSimpleFunc {

    @Param  ${type}Holder left;
    @Param  ${type}Holder right;
    @Output IntervalDayHolder out;

    public void setup(RecordBatch b) {
    }

    public void eval() {
        <#if type == "Time">
        out.milliSeconds = left.value - right.value;
        <#elseif type == "Date">
        out.days = (int) ((left.value - right.value) / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        <#elseif type == "TimeStamp">
        long difference = (left.value - right.value);
        out.milliSeconds = (int) (difference % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        out.days = (int) (difference / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        <#elseif type == "TimeStampTZ">
        if (left.index != right.index) {
            // The two inputs are in different time zones convert one of them
            org.joda.time.DateTime leftInput = new org.joda.time.DateTime(left.value, org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.timezoneList[left.index]));
            // convert left timestamp to the time zone of the right timestamp
            left.value = (leftInput.withZone(org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.timezoneList[right.index]))).getMillis();
        }
        long difference = (left.value - right.value);
        out.milliSeconds = (int) (difference % org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        out.days = (int) (difference / org.apache.drill.exec.expr.fn.impl.DateUtility.daysToStandardMillis);
        </#if>
    }
}
</#list>