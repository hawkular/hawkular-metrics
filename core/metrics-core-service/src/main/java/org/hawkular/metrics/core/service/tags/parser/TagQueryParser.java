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

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class TagQueryParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.6", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, OR=6, AND=7, NOT=8, EQUAL=9, NOTEQUAL=10, 
		REGEXMATCH=11, NOTREGEXMATCH=12, IN=13, SIMPLETEXT=14, COMPLEXTEXT=15, 
		WS=16;
	public static final int
		RULE_tagquery = 0, RULE_object = 1, RULE_pair = 2, RULE_logical_operator = 3, 
		RULE_boolean_operator = 4, RULE_regex_operator = 5, RULE_array_operator = 6, 
		RULE_existence_operator = 7, RULE_array = 8, RULE_value = 9, RULE_key = 10;
	public static final String[] ruleNames = {
		"tagquery", "object", "pair", "logical_operator", "boolean_operator", 
		"regex_operator", "array_operator", "existence_operator", "array", "value", 
		"key"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'['", "','", "']'", null, null, null, "'='", "'!='", 
		"'~'", "'!~'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, "OR", "AND", "NOT", "EQUAL", "NOTEQUAL", 
		"REGEXMATCH", "NOTREGEXMATCH", "IN", "SIMPLETEXT", "COMPLEXTEXT", "WS"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "TagQuery.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public TagQueryParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class TagqueryContext extends ParserRuleContext {
		public ObjectContext object() {
			return getRuleContext(ObjectContext.class,0);
		}
		public TagqueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tagquery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterTagquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitTagquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitTagquery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TagqueryContext tagquery() throws RecognitionException {
		TagqueryContext _localctx = new TagqueryContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_tagquery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(22);
			object(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ObjectContext extends ParserRuleContext {
		public PairContext pair() {
			return getRuleContext(PairContext.class,0);
		}
		public List<ObjectContext> object() {
			return getRuleContexts(ObjectContext.class);
		}
		public ObjectContext object(int i) {
			return getRuleContext(ObjectContext.class,i);
		}
		public Logical_operatorContext logical_operator() {
			return getRuleContext(Logical_operatorContext.class,0);
		}
		public ObjectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_object; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitObject(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitObject(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ObjectContext object() throws RecognitionException {
		return object(0);
	}

	private ObjectContext object(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ObjectContext _localctx = new ObjectContext(_ctx, _parentState);
		ObjectContext _prevctx = _localctx;
		int _startState = 2;
		enterRecursionRule(_localctx, 2, RULE_object, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(30);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NOT:
			case SIMPLETEXT:
				{
				setState(25);
				pair();
				}
				break;
			case T__0:
				{
				setState(26);
				match(T__0);
				setState(27);
				object(0);
				setState(28);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(38);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,1,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new ObjectContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_object);
					setState(32);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(33);
					logical_operator();
					setState(34);
					object(2);
					}
					} 
				}
				setState(40);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,1,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PairContext extends ParserRuleContext {
		public KeyContext key() {
			return getRuleContext(KeyContext.class,0);
		}
		public Existence_operatorContext existence_operator() {
			return getRuleContext(Existence_operatorContext.class,0);
		}
		public Boolean_operatorContext boolean_operator() {
			return getRuleContext(Boolean_operatorContext.class,0);
		}
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public Regex_operatorContext regex_operator() {
			return getRuleContext(Regex_operatorContext.class,0);
		}
		public Array_operatorContext array_operator() {
			return getRuleContext(Array_operatorContext.class,0);
		}
		public ArrayContext array() {
			return getRuleContext(ArrayContext.class,0);
		}
		public PairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pair; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterPair(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitPair(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitPair(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PairContext pair() throws RecognitionException {
		PairContext _localctx = new PairContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_pair);
		try {
			setState(57);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(41);
				key();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(42);
				existence_operator();
				setState(43);
				key();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(45);
				key();
				setState(46);
				boolean_operator();
				setState(47);
				value();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(49);
				key();
				setState(50);
				regex_operator();
				setState(51);
				value();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(53);
				key();
				setState(54);
				array_operator();
				setState(55);
				array();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Logical_operatorContext extends ParserRuleContext {
		public TerminalNode AND() { return getToken(TagQueryParser.AND, 0); }
		public TerminalNode OR() { return getToken(TagQueryParser.OR, 0); }
		public Logical_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logical_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterLogical_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitLogical_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitLogical_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Logical_operatorContext logical_operator() throws RecognitionException {
		Logical_operatorContext _localctx = new Logical_operatorContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_logical_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(59);
			_la = _input.LA(1);
			if ( !(_la==OR || _la==AND) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Boolean_operatorContext extends ParserRuleContext {
		public TerminalNode EQUAL() { return getToken(TagQueryParser.EQUAL, 0); }
		public TerminalNode NOTEQUAL() { return getToken(TagQueryParser.NOTEQUAL, 0); }
		public Boolean_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_boolean_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterBoolean_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitBoolean_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitBoolean_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Boolean_operatorContext boolean_operator() throws RecognitionException {
		Boolean_operatorContext _localctx = new Boolean_operatorContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_boolean_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(61);
			_la = _input.LA(1);
			if ( !(_la==EQUAL || _la==NOTEQUAL) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Regex_operatorContext extends ParserRuleContext {
		public TerminalNode REGEXMATCH() { return getToken(TagQueryParser.REGEXMATCH, 0); }
		public TerminalNode NOTREGEXMATCH() { return getToken(TagQueryParser.NOTREGEXMATCH, 0); }
		public Regex_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_regex_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterRegex_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitRegex_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitRegex_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Regex_operatorContext regex_operator() throws RecognitionException {
		Regex_operatorContext _localctx = new Regex_operatorContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_regex_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(63);
			_la = _input.LA(1);
			if ( !(_la==REGEXMATCH || _la==NOTREGEXMATCH) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Array_operatorContext extends ParserRuleContext {
		public TerminalNode IN() { return getToken(TagQueryParser.IN, 0); }
		public TerminalNode NOT() { return getToken(TagQueryParser.NOT, 0); }
		public Array_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterArray_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitArray_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitArray_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Array_operatorContext array_operator() throws RecognitionException {
		Array_operatorContext _localctx = new Array_operatorContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_array_operator);
		try {
			setState(68);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IN:
				enterOuterAlt(_localctx, 1);
				{
				setState(65);
				match(IN);
				}
				break;
			case NOT:
				enterOuterAlt(_localctx, 2);
				{
				setState(66);
				match(NOT);
				setState(67);
				match(IN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Existence_operatorContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(TagQueryParser.NOT, 0); }
		public Existence_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_existence_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterExistence_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitExistence_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitExistence_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Existence_operatorContext existence_operator() throws RecognitionException {
		Existence_operatorContext _localctx = new Existence_operatorContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_existence_operator);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(70);
			match(NOT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArrayContext extends ParserRuleContext {
		public List<ValueContext> value() {
			return getRuleContexts(ValueContext.class);
		}
		public ValueContext value(int i) {
			return getRuleContext(ValueContext.class,i);
		}
		public ArrayContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitArray(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitArray(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArrayContext array() throws RecognitionException {
		ArrayContext _localctx = new ArrayContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_array);
		int _la;
		try {
			setState(85);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(72);
				match(T__2);
				setState(73);
				value();
				setState(78);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(74);
					match(T__3);
					setState(75);
					value();
					}
					}
					setState(80);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(81);
				match(T__4);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(83);
				match(T__2);
				setState(84);
				match(T__4);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueContext extends ParserRuleContext {
		public TerminalNode SIMPLETEXT() { return getToken(TagQueryParser.SIMPLETEXT, 0); }
		public TerminalNode COMPLEXTEXT() { return getToken(TagQueryParser.COMPLEXTEXT, 0); }
		public ValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueContext value() throws RecognitionException {
		ValueContext _localctx = new ValueContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(87);
			_la = _input.LA(1);
			if ( !(_la==SIMPLETEXT || _la==COMPLEXTEXT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeyContext extends ParserRuleContext {
		public TerminalNode SIMPLETEXT() { return getToken(TagQueryParser.SIMPLETEXT, 0); }
		public KeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_key; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).enterKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof TagQueryListener ) ((TagQueryListener)listener).exitKey(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TagQueryVisitor ) return ((TagQueryVisitor<? extends T>)visitor).visitKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeyContext key() throws RecognitionException {
		KeyContext _localctx = new KeyContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_key);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(89);
			match(SIMPLETEXT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 1:
			return object_sempred((ObjectContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean object_sempred(ObjectContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\22^\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4"+
		"\f\t\f\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\5\3!\n\3\3\3\3\3\3\3\3\3\7\3\'"+
		"\n\3\f\3\16\3*\13\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\5\4<\n\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\b\5\bG\n\b\3"+
		"\t\3\t\3\n\3\n\3\n\3\n\7\nO\n\n\f\n\16\nR\13\n\3\n\3\n\3\n\3\n\5\nX\n"+
		"\n\3\13\3\13\3\f\3\f\3\f\2\3\4\r\2\4\6\b\n\f\16\20\22\24\26\2\6\3\2\b"+
		"\t\3\2\13\f\3\2\r\16\3\2\20\21[\2\30\3\2\2\2\4 \3\2\2\2\6;\3\2\2\2\b="+
		"\3\2\2\2\n?\3\2\2\2\fA\3\2\2\2\16F\3\2\2\2\20H\3\2\2\2\22W\3\2\2\2\24"+
		"Y\3\2\2\2\26[\3\2\2\2\30\31\5\4\3\2\31\3\3\2\2\2\32\33\b\3\1\2\33!\5\6"+
		"\4\2\34\35\7\3\2\2\35\36\5\4\3\2\36\37\7\4\2\2\37!\3\2\2\2 \32\3\2\2\2"+
		" \34\3\2\2\2!(\3\2\2\2\"#\f\3\2\2#$\5\b\5\2$%\5\4\3\4%\'\3\2\2\2&\"\3"+
		"\2\2\2\'*\3\2\2\2(&\3\2\2\2()\3\2\2\2)\5\3\2\2\2*(\3\2\2\2+<\5\26\f\2"+
		",-\5\20\t\2-.\5\26\f\2.<\3\2\2\2/\60\5\26\f\2\60\61\5\n\6\2\61\62\5\24"+
		"\13\2\62<\3\2\2\2\63\64\5\26\f\2\64\65\5\f\7\2\65\66\5\24\13\2\66<\3\2"+
		"\2\2\678\5\26\f\289\5\16\b\29:\5\22\n\2:<\3\2\2\2;+\3\2\2\2;,\3\2\2\2"+
		";/\3\2\2\2;\63\3\2\2\2;\67\3\2\2\2<\7\3\2\2\2=>\t\2\2\2>\t\3\2\2\2?@\t"+
		"\3\2\2@\13\3\2\2\2AB\t\4\2\2B\r\3\2\2\2CG\7\17\2\2DE\7\n\2\2EG\7\17\2"+
		"\2FC\3\2\2\2FD\3\2\2\2G\17\3\2\2\2HI\7\n\2\2I\21\3\2\2\2JK\7\5\2\2KP\5"+
		"\24\13\2LM\7\6\2\2MO\5\24\13\2NL\3\2\2\2OR\3\2\2\2PN\3\2\2\2PQ\3\2\2\2"+
		"QS\3\2\2\2RP\3\2\2\2ST\7\7\2\2TX\3\2\2\2UV\7\5\2\2VX\7\7\2\2WJ\3\2\2\2"+
		"WU\3\2\2\2X\23\3\2\2\2YZ\t\5\2\2Z\25\3\2\2\2[\\\7\20\2\2\\\27\3\2\2\2"+
		"\b (;FPW";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}