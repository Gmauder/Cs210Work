package apps;

import static sql.FieldType.BOOLEAN;
import static sql.FieldType.INTEGER;
import static sql.FieldType.STRING;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import sql.QueryError;
import tables.SearchTable;
import tables.Table;

/**
 * Implements a user console for
 * interacting with a database.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public class Console {
	
	
	/*
	 * TODO: Implement stub for Module 3.
	 */

	/**
	 * The entry point for execution
	 * with user input/output.
	 */
	public static void main(String[] args) {
		
		
		
		try (
			final Database db = new Database(true);
			final Scanner in = new Scanner(System.in);
			final PrintStream out = System.out;
		) {
			
			Table table = new SearchTable(
					"sandbox_1",
					List.of("letter", "order", "vowel"),
					List.of(STRING, INTEGER, BOOLEAN),
					0
				);

				table.put(List.of("alpha", 1, true));
				table.put(List.of("beta", 2, false));
				table.put(List.of("gamma", 3, false));
				table.put(List.of("delta", 4, false));
				table.put(List.of("tau", 19, false));
				table.put(List.of("pi", 16, false));
				table.put(List.of("omega", 24, true));
				table.put(Arrays.asList("N/A", null, null));

				db.create(table);
				//System.out.println(table);
			//REPL
			//read-evaluate-print-loop
			
			{//TO DO: make this a loop which stops on input EXIT
				
			//result tables always start with an underscore
				
				boolean runs = true;
				
				while(runs) {
				
			out.print(">> ");

			// echo "hello, world!" 
			// echo "hello, world!"; echo "goodbye, world!" 
			
			String script = in.nextLine().strip();
			
			//if the input is a comment, skip to next run of REPL
			
			String[] queries = script.split(";");
			// >> -- this is a comment
			
			// echo "hi"; echo "bye"
			//
			//Query: echo "hi"
			//Result: hi
			//
			//Query: echo "bye"
			//Result: bye
			
			for(String query: queries) {
				query = query.strip();
				
				if(!query.isBlank()) {
				if(query.startsWith("--")){
					
				}
				else {
				//if the query is blank, skip to the next run of the loop
				//print the query
					out.println("Query: " + query);
				if(query.toLowerCase().equals("exit")) {
					runs = false;
					break;
				}
				
			try {
				Object res = db.interpret(query);
				
				//use instanceof to check the type
				//branch accordingly
				if(res instanceof Table) {
					if(((Table) res).getTableName().startsWith("_")) {
						out.println("Result Set: \n" + res.toString());
						
					}
					else {
						out.println("Table: \n" + res.toString());
					}
				}
				else if(res instanceof Integer) {
					out.println("Rows Affected: " + res);
				}
				else if(res instanceof String || res instanceof Boolean) {
					out.println("Result: " + res);
				}
				
				else{
					out.println("Result: " + res);
				}
			}
			catch (QueryError e) {
				
				out.println("Error: " + e);
			}
		}
			}
				out.println();
			}
			}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
