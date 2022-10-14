package apps;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

import sql.QueryError;

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
			//REPL
			//read-evaluate-print-loop
			
			{//TO DO: make this a loop which stops on input EXIT
				
			//result tables always start with an underscore
				
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
				//if the query is blank, skip to the next run of the loop
				//print the query
				if(query.toLowerCase().equals("exit")) {
					break;
				}
				if(!query.isBlank()) {
			try {
				Object res = db.interpret(query);
				//use instanceof to check the type
				//branch accordingly
				out.println("Result: " + res);
			}
			catch (QueryError e) {
				out.println("Error: " + e);
			}
		}
			}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
