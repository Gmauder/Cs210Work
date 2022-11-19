package drivers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static sql.FieldType.*;

import apps.Database;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;
import tables.Table;

/*
 * ECHO "Hello, world!"
 * 	 -> string: Hello, world!
 */
public class InsertInto implements Driver {
	private static final Pattern queryPattern = Pattern.compile(
			//sanitizes the keywords, table name, column names, stuff (for literals)
			// group 1: keyword
			//2: table name
			//3: column names list (or null)
			//4: literal values list (which hasn't been sanitized)
		//old//(INSERT|REPLACE)\s*INTO\s*([a-z][a-z0-9_]*)\s*(?:\(((?:(?:[a-z][a-z0-9_]*,?)\s*)*)\))?\s*VALUES\s*\(((?:[a-z0-9_\\"]+,?\s*)*)\)
			//(INSERT|REPLACE)\s*INTO\s*([a-z][a-z0-9_]*)\s*(?:\(((?:(?:[a-z][a-z0-9_]*,?)\s*)*)\))?\s*VALUES\s*\(((?:[ !?\-+a-z0-9_\\"]+,?\s*)*)\)
		"(INSERT|REPLACE)\\s*INTO\\s*([a-z][a-z0-9_]*)\\s*(?:\\(((?:(?:[a-z][a-z0-9_]*,?)\\s*)*)\\))?\\s*VALUES\\s*\\(((?:[ !?\\-+a-z0-9_\\\\\"]+,?\\s*)*)\\)",
		Pattern.CASE_INSENSITIVE
	);
	
	private static final Pattern literalPattern = Pattern.compile(
			//string format|integer format|boolean format|null format
			//each of which has a capture group
		//"([^"]*)"|(?:\+)?([-0-9]+)|(true|false)|(null)
		"\"([^\"]*)\"|(?:\\+)?([-0-9]+)|(true|false)|(null)",
		Pattern.CASE_INSENSITIVE
	);

	//group1: insert vs replace
	//mode which supports duplicate keys (replace)
	//mode which does not (insert)
	
	//group 2: name of the table
	
	//group 3: either null (short form)
	//or else a comma separated list of column names (long form)

	//group 4: depends on strategy
	//group 4: is stuff (any non parenthesis content)
	
	
	
	//field for whether short/long form(boolean flag)
	boolean isShort;
	//field for table name
	String tName;
	//field for the literal values(array of strings)
	String[] litValues;
	//field for the column names(array of strings)
	String[] litTypes;
	
	String[]colNames;
	//field for whether insert or replace mode(boolean flag)
	boolean isInsert;
	
	FieldType[] valTypes;
	@Override
	public boolean parse(String query) {
		Matcher matcher = queryPattern.matcher(query.strip());
		if (!matcher.matches())
			return false;
		
		
		isInsert = (matcher.group(1).equalsIgnoreCase("Insert"));
		// assign a mode flag equal to whether or not 
		//the insert keyword was used (vs replace keyword)
		//eg. insertMode, requireUniqueKeys, isInsert
		//eg. replaceMode, allowDuplicateKeys, isReplace
		
		//get the table name
		tName = matcher.group(2);
		
		//check if group 3 is null:
			//set the short/long form flag to show that its short form
		//otherwise:
			//set the short/long form flag to show that its long form
			//split group 3 on commas into a field for column names
		
		if(Objects.equals(matcher.group(3), null)) {
			isShort = true;
		}
		else {
			isShort = false;
			colNames = matcher.group(3).split(",");
			for(int i = 0; i< colNames.length; i++)
			colNames[i] = colNames[i].strip();
		}
		
		
		//split group 4 on commas into a field for the literal values
		//eg. [""A"","1","true"] (raw characters for each
	
		litValues = matcher.group(4).split(",");
		
		
		

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		
		Table table = db.find(tName);
		if(table == null) {
			throw new QueryError("table is null");
		}
		List<String> columnNames = table.getColumnNames();
		List<FieldType> columnTypes = table.getColumnTypes();
		int prime = table.getPrimaryIndex();
		
		//Phase 1 - find a correspondence between
		//col names in the query and col names in the schema
		ArrayList<Integer> pointerList = new ArrayList<Integer>();
		//if short form:
		if(isShort) {
			
			int counter = 0;
			for(var col : columnNames) {
				pointerList.add(counter);
				
				counter++;
				
			}
		}
		else {
			int j = 0;
			for(var col: colNames) {
				//System.out.println(col);
				j = columnNames.indexOf(col);
				if(j == -1) {
					throw new QueryError("that column does not exist in the schema");
				}
				if(pointerList.contains(j)) {
					throw new QueryError("j is already contained in the pointers list");
				}
				pointerList.add(j);
			}
			if(!pointerList.contains(prime)) {
				throw new QueryError("The primary index is not contained in the pointer list");
			}
		}
		//	build the pointers list to exactly match the schema
		//else if long form:
		//	build the pointers list with the correspondence from the query {
		// 		for each query column name:
		//			j (real schema index) = index of name in schema
		//			if j was -1: throw a query error
		//			//if j is already contained in the pointers list: throw a query error
		//			//add the j index to the end of the pointers list
		// }
		//		if the primary index isnt contained in the pointers list: throw a query error
		
		//Phase 2:
		if(litValues.length != pointerList.size()) {
			throw new QueryError("There are a different number of values and columns");
		}
		// based on the correspondence already found
		
		
		
		//If # of literals does not match number of columns in query(or pointers list) throw a query error
		
		//initialize a counter for the number of rows to 0
	int rowCounter = 0;
	//make a new empty row and fill it with nulls up to the number of columns in the schema
	ArrayList<Object> nRow = new ArrayList<Object>();
	ArrayList<Object> rowList = new ArrayList<Object>();
	
	for(var col : columnNames) {
		nRow.add(null);
	}
	int ncount = 0;
	//for each index i (for each name that comes from the query)
	for(String n : colNames)
	{
	
	int j = pointerList.get(ncount);
	String name = columnNames.get(j);
	FieldType type = columnTypes.get(j);
	String litValue = litValues[ncount];
	ncount++;
	for(int i = 0; i < litValues.length; i++){
		litValues[i] = litValues[i].strip();
	}
	
	//	let j index be the corresponding schema index for the query index i
	//	let the name come from the index j in the schemas column names list
	//	let the type come from index j in the schemas column list
	//	let the literal value be the literal index at i in the literals array/list
	
	//	let there be an uninitialized local variable for the resulting value (use Object type)
	Matcher litMatcher = literalPattern.matcher(litValue);
	Object result;
	//	
	
	
	if(!litMatcher.matches()) {
		throw new QueryError("no matches");
	}
		
	//	if the literal doesnt match any of the available types:
	//		throw a query error
	//	
		if((!Objects.equals(litMatcher.group(1), null)) && type.equals(STRING) ) {
			if(litMatcher.group(1).length() > 127) {
				throw new QueryError("The string is too long");
			}
			result = litMatcher.group(1);
		}
	//	if the literal value matches string AND the schema type is also string
	//		if the string is too long throw a query error
	//		just use your regex with a capture group that doesnt include quotation marks to get the non-quoted chars
	//		assign the string as the local variable for the resulting value
		else if(!Objects.equals(litMatcher.group(2), null) && type.equals(INTEGER)) {
			
			try
		    {   
		       result = Integer.parseInt(litMatcher.group(2));
		           
		    }
		    catch(Exception e)
		    {
		        throw new QueryError("Literal cannot be parsed as an int");
		    }
				if(litMatcher.group(2).charAt(0) == '0' && litMatcher.group(2).length() > 1) {
					throw new QueryError("Integer can not start with 0");
				}
			
		}
	//	else if the literal value matches integer AND the schema type is also integer
	//		parse the literal as an integer (even if it has a sign) if it cant be parsed throw a query error
	//		assign the parsed integer as the local variable for the resulting value (Integer Object)
		
		else if((!Objects.equals(litMatcher.group(3), null)) && type.equals(BOOLEAN)) {
			try
		    {   
		       result = Boolean.parseBoolean(litMatcher.group(3));
		           
		    }
		    catch(Exception e)
		    {
		        throw new QueryError("Literal cannot be parsed as an boolean");
		    }
		}
		else if(Objects.equals(litMatcher.group(3), "null")) {
			result = null;
		}
		else {
			throw new QueryError("This is not cool");
		}
	//	else if the literal value matches boolean AND the schema type is also boolean
	//		parse the literal as a boolean
	//		assign the parsed boolean as the local variable for the resulting value (Boolean Object)
	//	else if the literal value matches null:
	//		assign a null as the local variable for the resulting value
		if(j == prime) {
			if(result == null) {
				throw new QueryError("Cannot add null to the primary index");
			}
		}
	//	if the j index corresponds to the primary
	//		if the value to be added to the row is null:
	//			throw a query error
		
		
	//			
		for(var row: table.rows()) {
			System.out.println(isInsert);
			System.out.println(row.get(0));
			System.out.println(result.toString());
		if(isInsert && row.get(0).equals(result.toString())) {
			
			throw new QueryError("the table already contains that value");
		}
		}
	//		if we are in insert mode AND the table already contains the value as one of its row keys:
	//			throw a query error
		nRow.set(j, result);
	//	assign the jth element of the row to have the resulting value that we sanitized
	}
	
	
			table.put(nRow);
			rowCounter++;
	
	//	put the row in the table 
	//	add 1 to the rows counter
	
	//at the end of execute:
	//return the number of rows that were inserted/replaced
		
	//	*/
		return rowCounter;
	}
}
