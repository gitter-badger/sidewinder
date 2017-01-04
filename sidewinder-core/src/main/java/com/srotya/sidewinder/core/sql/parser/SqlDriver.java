package com.srotya.sidewinder.core.sql.parser;

import java.util.Iterator;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.srotya.sidewinder.core.sql.operators.ComplexOperator;
import com.srotya.sidewinder.core.sql.operators.Operator;
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
		
		Operator tree = listener.getFilterTree();
		prune(tree);
	}

	
	public static void prune(Operator tree) {
		if(tree instanceof ComplexOperator) {
			List<Operator> operators = ((ComplexOperator) tree).getOperators();
			Iterator<Operator> iterator = operators.iterator();
			while(iterator.hasNext()) {
				Operator op = iterator.next();
				if(op.getClass().equals(tree.getClass())) {
					((ComplexOperator) tree).addOperator(op);
				}
			}
		}
	}
}
