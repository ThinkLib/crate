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

package io.crate.expression;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.aggregation.AggregationContext;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.reference.GatheringRefResolver;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory which can be used to create {@link Input}s from symbols.
 *
 * <p>
 *     Some inputs will be linked to some kind of expression.
 *
 *     E.g. InputColumns will have a corresponding {@code CollectExpression<Row, ?>}.
 *     The state on these CollectExpression needs to be set in order for {@link Input#value()} to return the correct value.
 *     <br />
 *
 *     Inputs from symbols like Functions or Literals are "standalone" and won't have a linked expression.
 * </p>
 *
 *
 * <p>
 *      Note:
 *      Symbols may either contain References OR InputColumn when using {@link #ctxForRefs(ReferenceResolver)} or
 *      {@link #ctxForInputColumns()}.
 *
 *      This is due to the fact that References will result in a different kind of Expression class than InputColumns do.
 *      The expression class for references depends on the used ReferenceResolver.
 *
 *      If the Symbols contains References and InputColumns, please use
 *      {@link #ctxForRefsWithInputCols(ReferenceResolver)}.
 *
 * </p>
 */
public class InputFactory {

    private final Functions functions;

    public InputFactory(Functions functions) {
        this.functions = functions;
    }

    public <T extends Input<?>> Context<T> ctxForRefs(ReferenceResolver<? extends T> referenceResolver) {
        List<T> expressions = new ArrayList<>();
        return new Context<>(expressions,
            new RefVisitor<>(
                functions,
                new GatheringRefResolver<>(expressions::add, referenceResolver)));
    }

    public Context<CollectExpression<Row, ?>> ctxForInputColumns() {
        List<CollectExpression<Row, ?>> expressions = new ArrayList<>();
        return new Context<>(expressions, new InputColumnVisitor(functions, expressions));
    }

    public Context<CollectExpression<Row, ?>> ctxForInputColumns(Iterable<? extends Symbol> symbols) {
        Context<CollectExpression<Row, ?>> context = ctxForInputColumns();
        context.add(symbols);
        return context;
    }

    public Context<CollectExpression<Row, ?>> ctxForAggregations() {
        List<CollectExpression<Row, ?>> expressions = new ArrayList<>();
        List<AggregationContext> aggregationContexts = new ArrayList<>();
        return new Context<>(
            expressions,
            aggregationContexts,
            new AggregationVisitor(functions, expressions, aggregationContexts));
    }

    public <T extends Input<?>> ContextInputAware<T> ctxForRefsWithInputCols(ReferenceResolver<? extends T> referenceResolver) {
        List<T> expressions = new ArrayList<>();
        return new ContextInputAware<>(expressions,
            new RefVisitorWithInputColumns<>(
                functions,
                new GatheringRefResolver<>(expressions::add, referenceResolver)
            )
        );
    }

    public static class Context<T extends Input<?>> {

        private final List<Input<?>> topLevelInputs = new ArrayList<>();
        private final List<T> expressions;
        private final List<AggregationContext> aggregationContexts;
        private final SymbolVisitor<?, Input<?>> visitor;


        private Context(List<T> expressions,
                        List<AggregationContext> aggregationContexts,
                        SymbolVisitor<?, Input<?>> visitor) {
            this.expressions = expressions;
            this.aggregationContexts = aggregationContexts;
            this.visitor = visitor;
        }

        private Context(List<T> expressions, SymbolVisitor<?, Input<?>> visitor) {
            this(expressions, Collections.emptyList(), visitor);
        }

        public List<Input<?>> topLevelInputs() {
            return topLevelInputs;
        }

        public List<T> expressions() {
            return expressions;
        }

        /**
         * Create inputs for all symbols, The inputs will the added to {@link #topLevelInputs()}
         */
        public void add(Iterable<? extends Symbol> symbols) {
            for (Symbol symbol : symbols) {
                Input<?> input = add(symbol);
                if (input != null) {
                    topLevelInputs.add(input);
                }
            }
        }

        /**
         * Create an input from a symbol.
         * If the input contains any linked expressions they'll be available in {@link #expressions()}.
         * <p>
         * The Input WON'T be part of {@link #topLevelInputs()}
         * </p>
         */
        public Input<?> add(Symbol symbol) {
            return visitor.process(symbol, null);
        }

        public List<AggregationContext> aggregations() {
            return aggregationContexts;
        }
    }

    public static class ContextInputAware<T extends Input<?>> extends Context<T> {

        private final RefVisitorWithInputColumns<?> visitor;

        private ContextInputAware(List<T> expressions, RefVisitorWithInputColumns<?> visitor) {
            super(expressions, visitor);
            this.visitor = visitor;
        }

        public List<CollectExpression<Row, ?>> inputColExpressions() {
            return visitor.getInputColExpressions();
        }
    }

    private static class InputColumnVisitor extends BaseImplementationSymbolVisitor<Void> {

        private final List<CollectExpression<Row, ?>> expressions;
        private final IntObjectMap<InputCollectExpression> inputCollectExpressions = new IntObjectHashMap<>();

        InputColumnVisitor(Functions functions, List<CollectExpression<Row, ?>> expressions) {
            super(functions);
            this.expressions = expressions;
        }

        @Override
        public Input<?> visitInputColumn(InputColumn inputColumn, Void context) {
            int index = inputColumn.index();
            InputCollectExpression inputCollectExpression = inputCollectExpressions.get(index);
            if (inputCollectExpression == null) {
                inputCollectExpression = new InputCollectExpression(index);
                inputCollectExpressions.put(index, inputCollectExpression);
                expressions.add(inputCollectExpression);
            }
            return inputCollectExpression;
        }
    }

    private static class AggregationVisitor extends InputColumnVisitor {

        private final List<AggregationContext> aggregationContexts;

        AggregationVisitor(Functions functions,
                           List<CollectExpression<Row, ?>> expressions,
                           List<AggregationContext> aggregationContexts) {
            super(functions, expressions);
            this.aggregationContexts = aggregationContexts;
        }

        @Override
        public Input<?> visitAggregation(Aggregation symbol, Void context) {
            FunctionImplementation impl = functions.getQualified(symbol.functionIdent());
            AggregationContext aggregationContext = new AggregationContext((AggregationFunction) impl);
            for (Symbol aggInput : symbol.inputs()) {
                aggregationContext.addInput(process(aggInput, context));
            }
            aggregationContexts.add(aggregationContext);

            // can't generate an input from an aggregation.
            // since they cannot/shouldn't be nested this should be okay.
            return null;
        }
    }

    private static class RefVisitor<T extends Input<?>> extends BaseImplementationSymbolVisitor<Void> {

        private final ReferenceResolver<T> referenceResolver;
        private final Map<Reference, T> referenceMap;

        RefVisitor(Functions functions, ReferenceResolver<T> referenceResolver) {
            super(functions);
            this.referenceResolver = referenceResolver;
            this.referenceMap = new HashMap<>();
        }

        @Override
        public Input<?> visitReference(Reference ref, Void context) {
            T implementation = referenceMap.get(ref);
            if (implementation != null) {
                return implementation;
            }
            implementation = referenceResolver.getImplementation(ref);
            if (implementation == null) {
                throw new IllegalArgumentException("Column implementation not found for: " + ref);
            }
            referenceMap.put(ref, implementation);
            return implementation;
        }
    }

    private static class RefVisitorWithInputColumns<T extends Input<?>> extends RefVisitor<T> {

        private final List<CollectExpression<Row, ?>> inputColExpressions = new ArrayList<>();
        private final IntObjectMap<InputCollectExpression> inputCollectExpressions = new IntObjectHashMap<>();

        RefVisitorWithInputColumns(Functions functions, ReferenceResolver<T> referenceResolver) {
            super(functions, referenceResolver);
        }

        List<CollectExpression<Row, ?>> getInputColExpressions() {
            return inputColExpressions;
        }

        @Override
        public Input<?> visitInputColumn(InputColumn inputColumn, Void context) {
            int index = inputColumn.index();
            InputCollectExpression inputCollectExpression = inputCollectExpressions.get(index);
            if (inputCollectExpression == null) {
                inputCollectExpression = new InputCollectExpression(index);
                inputCollectExpressions.put(index, inputCollectExpression);
                inputColExpressions.add(inputCollectExpression);
            }
            return inputCollectExpression;
        }
    }
}
