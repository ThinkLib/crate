/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.window;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.breaker.RamAccountingContext;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.SqlExpressions;
import io.crate.testing.TestingBatchIterators;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;


class WindowFunctionTest {

    private SqlExpressions sqlExpressions;
    private Functions functions;
    private Map<QualifiedName, AnalyzedRelation> tableSources;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private InputFactory inputFactory;

    private final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));

    @Before
    public void prepareFunctions() throws Exception {
        DocTableInfo tableInfo = TestingTableInfo.builder(new RelationName(DocSchemaInfo.NAME, "users"), null)
            .add("id", DataTypes.INTEGER)
            .add("name", DataTypes.STRING)
            .add("tags", new ArrayType(DataTypes.STRING))
            .add("age", DataTypes.INTEGER)
            .add("a", DataTypes.INTEGER)
            .add("x", DataTypes.LONG)
            .add("shape", DataTypes.GEO_SHAPE)
            .add("timestamp", DataTypes.TIMESTAMP)
            .add("timezone", DataTypes.STRING)
            .add("interval", DataTypes.STRING)
            .add("time_format", DataTypes.STRING)
            .add("long_array", new ArrayType(DataTypes.LONG))
            .add("int_array", new ArrayType(DataTypes.INTEGER))
            .add("array_string_array", new ArrayType(new ArrayType(DataTypes.STRING)))
            .add("long_set", new SetType(DataTypes.LONG))
            .add("regex_pattern", DataTypes.STRING)
            .add("geoshape", DataTypes.GEO_SHAPE)
            .add("geopoint", DataTypes.GEO_POINT)
            .add("geostring", DataTypes.STRING)
            .add("is_awesome", DataTypes.BOOLEAN)
            .add("double_val", DataTypes.DOUBLE)
            .add("float_val", DataTypes.DOUBLE)
            .add("short_val", DataTypes.SHORT)
            .add("obj", DataTypes.OBJECT, ImmutableList.of())
            .build();
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        tableSources = ImmutableMap.of(new QualifiedName("users"), tableRelation);
        sqlExpressions = new SqlExpressions(tableSources);
        functions = sqlExpressions.functions();
        inputFactory = new InputFactory(functions);
    }


    private void assertInputAndExpectedSize(Map<String, List<Literal>> inputValueMap, Object[] expectedValues) {
        assertThat("No Input Provided", inputValueMap.isEmpty(), is(false));

        List<Integer> inputSizes = inputValueMap.values()
            .stream()
            .map(l -> l.size())
            .distinct()
            .collect(Collectors.toList()
            );
        assertThat("Input Lists need to be of equal size",
            inputSizes.size(),
            is(1));
        assertThat("Expected Values need to be of equal size wrt the Input",
            expectedValues.length,
            is(inputSizes.get(0)));

    }

    protected void assertEvaluate(String functionExpression,
                               Map<String, List<Literal>> inputValueMap,
                               Object[] expectedValues) {
        // pre-asserts
        assertInputAndExpectedSize(inputValueMap, expectedValues);

        // parse input and analyze
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        assertThat(functionSymbol, instanceOf(io.crate.expression.symbol.WindowFunction.class));

        io.crate.expression.symbol.WindowFunction function = (io.crate.expression.symbol.WindowFunction) functionSymbol;

        // assert function arguments and types and
        // TBD

        // get function implementation
        FunctionImplementation impl = functions.getQualified(function.info().ident());
        WindowFunction windowFunction = (WindowFunction) impl;

        // iterator setup
        WindowBatchIterator iterator = new WindowBatchIterator(
            function.windowDefinition(),
            Collections.emptyList(),
            Collections.emptyList(),
            // tmp - this needs to be set from inputValueMap
            TestingBatchIterators.range(1, 5),
            //
            Collections.singletonList(windowFunction),
            Collections.singletonList(windowFunction.info().returnType()),
            RAM_ACCOUNTING_CONTEXT,
            new int[] {0}
        );

        // consume input and produce output
        List<Object> actualResult = new ArrayList<>();
        while (iterator.moveNext()) {
            actualResult.add(iterator.currentElement().get(0));
        }

        // assert output
        assertThat(actualResult, contains(expectedValues));
    }
}
