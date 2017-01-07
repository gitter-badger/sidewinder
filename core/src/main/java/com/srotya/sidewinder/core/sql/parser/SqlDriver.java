/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core.sql.parser;

import java.util.Iterator;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.srotya.sidewinder.core.sql.ComplexOperator;
import com.srotya.sidewinder.core.sql.Condition;
import com.srotya.sidewinder.core.sql.parser.SQLParser.SqlContext;


public class SqlDriver {

	public static void main(String[] args) {
		SQLLexer lexer = new SQLLexer(new ANTLRInputStream("select * from testseries where p>2 and u<2"));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		SQLParser parser = new SQLParser(tokens);
		SqlContext select_stmt = parser.sql();

		ParseTreeWalker walker = new ParseTreeWalker();
		SQLParserBaseListener listener = new SQLParserBaseListener();
		walker.walk(listener, select_stmt);
		
		Condition tree = listener.getFilterTree();
		prune(tree);
	}

	
	public static void prune(Condition tree) {
		if(tree instanceof ComplexOperator) {
			List<Condition> operators = ((ComplexOperator) tree).getOperators();
			Iterator<Condition> iterator = operators.iterator();
			while(iterator.hasNext()) {
				Condition op = iterator.next();
				if(op.getClass().equals(tree.getClass())) {
					((ComplexOperator) tree).addOperator(op);
				}
			}
		}
	}
}
