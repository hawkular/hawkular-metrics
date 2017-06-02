// Generated from org/hawkular/metrics/core/service/tags/parser/TagQuery.g4 by ANTLR 4.6
package org.hawkular.metrics.core.service.tags.parser;

/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link TagQueryParser}.
 */
public interface TagQueryListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#tagquery}.
	 * @param ctx the parse tree
	 */
	void enterTagquery(TagQueryParser.TagqueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#tagquery}.
	 * @param ctx the parse tree
	 */
	void exitTagquery(TagQueryParser.TagqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#object}.
	 * @param ctx the parse tree
	 */
	void enterObject(TagQueryParser.ObjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#object}.
	 * @param ctx the parse tree
	 */
	void exitObject(TagQueryParser.ObjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#pair}.
	 * @param ctx the parse tree
	 */
	void enterPair(TagQueryParser.PairContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#pair}.
	 * @param ctx the parse tree
	 */
	void exitPair(TagQueryParser.PairContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#logical_operator}.
	 * @param ctx the parse tree
	 */
	void enterLogical_operator(TagQueryParser.Logical_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#logical_operator}.
	 * @param ctx the parse tree
	 */
	void exitLogical_operator(TagQueryParser.Logical_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#boolean_operator}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_operator(TagQueryParser.Boolean_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#boolean_operator}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_operator(TagQueryParser.Boolean_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#regex_operator}.
	 * @param ctx the parse tree
	 */
	void enterRegex_operator(TagQueryParser.Regex_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#regex_operator}.
	 * @param ctx the parse tree
	 */
	void exitRegex_operator(TagQueryParser.Regex_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#array_operator}.
	 * @param ctx the parse tree
	 */
	void enterArray_operator(TagQueryParser.Array_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#array_operator}.
	 * @param ctx the parse tree
	 */
	void exitArray_operator(TagQueryParser.Array_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#existence_operator}.
	 * @param ctx the parse tree
	 */
	void enterExistence_operator(TagQueryParser.Existence_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#existence_operator}.
	 * @param ctx the parse tree
	 */
	void exitExistence_operator(TagQueryParser.Existence_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#array}.
	 * @param ctx the parse tree
	 */
	void enterArray(TagQueryParser.ArrayContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#array}.
	 * @param ctx the parse tree
	 */
	void exitArray(TagQueryParser.ArrayContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(TagQueryParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(TagQueryParser.ValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link TagQueryParser#key}.
	 * @param ctx the parse tree
	 */
	void enterKey(TagQueryParser.KeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link TagQueryParser#key}.
	 * @param ctx the parse tree
	 */
	void exitKey(TagQueryParser.KeyContext ctx);
}