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

import io.crate.expression.symbol.Literal;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class RowNumberWindowFunctionTest extends WindowFunctionTest {

    @Test
    public void testRowNumberFunction() throws Exception {
        Map<String, List<Literal>> input = new HashMap<>();
        // note: currently input is hardcoded in evaluation
        // however the intention is to be provided using this map
        input.put("a", IntStream.range(1, 5).boxed().map(n -> Literal.of(n)).collect(Collectors.toList()));
        //
        Object[] expected = new Object[] {1, 2, 3, 4};

        assertEvaluate("row_number() over(order by a)", input, expected);
    }
}
