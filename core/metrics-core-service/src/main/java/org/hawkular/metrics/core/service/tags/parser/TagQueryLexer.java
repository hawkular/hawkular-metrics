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

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class TagQueryLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.6", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, OR=6, AND=7, EQUAL=8, NOTEQUAL=9, 
		IN=10, NOTIN=11, KEY=12, VALUE=13, WS=14;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "OR", "AND", "EQUAL", "NOTEQUAL", 
		"IN", "NOTIN", "KEY", "VALUE", "WS", "ESC", "UNICODE", "HEX", "A", "B", 
		"C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", 
		"Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'['", "','", "']'", null, null, "'=='", "'!='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, "OR", "AND", "EQUAL", "NOTEQUAL", 
		"IN", "NOTIN", "KEY", "VALUE", "WS"
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


	public TagQueryLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "TagQuery.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\20\u00ce\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
		" \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
		"+\4,\t,\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\b\3\b\3"+
		"\b\3\b\3\t\3\t\3\t\3\n\3\n\3\n\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\r\6\r"+
		"y\n\r\r\r\16\rz\3\16\3\16\3\16\7\16\u0080\n\16\f\16\16\16\u0083\13\16"+
		"\3\16\3\16\3\17\6\17\u0088\n\17\r\17\16\17\u0089\3\17\3\17\3\20\3\20\3"+
		"\20\5\20\u0091\n\20\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\23\3\23"+
		"\3\24\3\24\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3\32"+
		"\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3!\3!\3\"\3\""+
		"\3#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3+\3,\3,\2\2-\3"+
		"\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37"+
		"\2!\2#\2%\2\'\2)\2+\2-\2/\2\61\2\63\2\65\2\67\29\2;\2=\2?\2A\2C\2E\2G"+
		"\2I\2K\2M\2O\2Q\2S\2U\2W\2\3\2!\6\2\62;C\\aac|\4\2))^^\5\2\13\f\17\17"+
		"\"\"\n\2))\61\61^^ddhhppttvv\5\2\62;CHch\4\2CCcc\4\2DDdd\4\2EEee\4\2F"+
		"Fff\4\2GGgg\4\2HHhh\4\2IIii\4\2JJjj\4\2KKkk\4\2LLll\4\2MMmm\4\2NNnn\4"+
		"\2OOoo\4\2PPpp\4\2QQqq\4\2RRrr\4\2SSss\4\2TTtt\4\2UUuu\4\2VVvv\4\2WWw"+
		"w\4\2XXxx\4\2YYyy\4\2ZZzz\4\2[[{{\4\2\\\\||\u00b5\2\3\3\2\2\2\2\5\3\2"+
		"\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21"+
		"\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2"+
		"\2\2\2\35\3\2\2\2\3Y\3\2\2\2\5[\3\2\2\2\7]\3\2\2\2\t_\3\2\2\2\13a\3\2"+
		"\2\2\rc\3\2\2\2\17f\3\2\2\2\21j\3\2\2\2\23m\3\2\2\2\25p\3\2\2\2\27s\3"+
		"\2\2\2\31x\3\2\2\2\33|\3\2\2\2\35\u0087\3\2\2\2\37\u008d\3\2\2\2!\u0092"+
		"\3\2\2\2#\u0098\3\2\2\2%\u009a\3\2\2\2\'\u009c\3\2\2\2)\u009e\3\2\2\2"+
		"+\u00a0\3\2\2\2-\u00a2\3\2\2\2/\u00a4\3\2\2\2\61\u00a6\3\2\2\2\63\u00a8"+
		"\3\2\2\2\65\u00aa\3\2\2\2\67\u00ac\3\2\2\29\u00ae\3\2\2\2;\u00b0\3\2\2"+
		"\2=\u00b2\3\2\2\2?\u00b4\3\2\2\2A\u00b6\3\2\2\2C\u00b8\3\2\2\2E\u00ba"+
		"\3\2\2\2G\u00bc\3\2\2\2I\u00be\3\2\2\2K\u00c0\3\2\2\2M\u00c2\3\2\2\2O"+
		"\u00c4\3\2\2\2Q\u00c6\3\2\2\2S\u00c8\3\2\2\2U\u00ca\3\2\2\2W\u00cc\3\2"+
		"\2\2YZ\7*\2\2Z\4\3\2\2\2[\\\7+\2\2\\\6\3\2\2\2]^\7]\2\2^\b\3\2\2\2_`\7"+
		".\2\2`\n\3\2\2\2ab\7_\2\2b\f\3\2\2\2cd\5A!\2de\5G$\2e\16\3\2\2\2fg\5%"+
		"\23\2gh\5? \2hi\5+\26\2i\20\3\2\2\2jk\7?\2\2kl\7?\2\2l\22\3\2\2\2mn\7"+
		"#\2\2no\7?\2\2o\24\3\2\2\2pq\5\65\33\2qr\5? \2r\26\3\2\2\2st\5? \2tu\5"+
		"\65\33\2uv\5? \2v\30\3\2\2\2wy\t\2\2\2xw\3\2\2\2yz\3\2\2\2zx\3\2\2\2z"+
		"{\3\2\2\2{\32\3\2\2\2|\u0081\7)\2\2}\u0080\5\37\20\2~\u0080\n\3\2\2\177"+
		"}\3\2\2\2\177~\3\2\2\2\u0080\u0083\3\2\2\2\u0081\177\3\2\2\2\u0081\u0082"+
		"\3\2\2\2\u0082\u0084\3\2\2\2\u0083\u0081\3\2\2\2\u0084\u0085\7)\2\2\u0085"+
		"\34\3\2\2\2\u0086\u0088\t\4\2\2\u0087\u0086\3\2\2\2\u0088\u0089\3\2\2"+
		"\2\u0089\u0087\3\2\2\2\u0089\u008a\3\2\2\2\u008a\u008b\3\2\2\2\u008b\u008c"+
		"\b\17\2\2\u008c\36\3\2\2\2\u008d\u0090\7^\2\2\u008e\u0091\t\5\2\2\u008f"+
		"\u0091\5!\21\2\u0090\u008e\3\2\2\2\u0090\u008f\3\2\2\2\u0091 \3\2\2\2"+
		"\u0092\u0093\7w\2\2\u0093\u0094\5#\22\2\u0094\u0095\5#\22\2\u0095\u0096"+
		"\5#\22\2\u0096\u0097\5#\22\2\u0097\"\3\2\2\2\u0098\u0099\t\6\2\2\u0099"+
		"$\3\2\2\2\u009a\u009b\t\7\2\2\u009b&\3\2\2\2\u009c\u009d\t\b\2\2\u009d"+
		"(\3\2\2\2\u009e\u009f\t\t\2\2\u009f*\3\2\2\2\u00a0\u00a1\t\n\2\2\u00a1"+
		",\3\2\2\2\u00a2\u00a3\t\13\2\2\u00a3.\3\2\2\2\u00a4\u00a5\t\f\2\2\u00a5"+
		"\60\3\2\2\2\u00a6\u00a7\t\r\2\2\u00a7\62\3\2\2\2\u00a8\u00a9\t\16\2\2"+
		"\u00a9\64\3\2\2\2\u00aa\u00ab\t\17\2\2\u00ab\66\3\2\2\2\u00ac\u00ad\t"+
		"\20\2\2\u00ad8\3\2\2\2\u00ae\u00af\t\21\2\2\u00af:\3\2\2\2\u00b0\u00b1"+
		"\t\22\2\2\u00b1<\3\2\2\2\u00b2\u00b3\t\23\2\2\u00b3>\3\2\2\2\u00b4\u00b5"+
		"\t\24\2\2\u00b5@\3\2\2\2\u00b6\u00b7\t\25\2\2\u00b7B\3\2\2\2\u00b8\u00b9"+
		"\t\26\2\2\u00b9D\3\2\2\2\u00ba\u00bb\t\27\2\2\u00bbF\3\2\2\2\u00bc\u00bd"+
		"\t\30\2\2\u00bdH\3\2\2\2\u00be\u00bf\t\31\2\2\u00bfJ\3\2\2\2\u00c0\u00c1"+
		"\t\32\2\2\u00c1L\3\2\2\2\u00c2\u00c3\t\33\2\2\u00c3N\3\2\2\2\u00c4\u00c5"+
		"\t\34\2\2\u00c5P\3\2\2\2\u00c6\u00c7\t\35\2\2\u00c7R\3\2\2\2\u00c8\u00c9"+
		"\t\36\2\2\u00c9T\3\2\2\2\u00ca\u00cb\t\37\2\2\u00cbV\3\2\2\2\u00cc\u00cd"+
		"\t \2\2\u00cdX\3\2\2\2\b\2z\177\u0081\u0089\u0090\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}