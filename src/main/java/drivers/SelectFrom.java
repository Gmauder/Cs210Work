package drivers;

import java.math.BigDecimal;
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
import tables.HashArrayTable;
import tables.Table;

/*
 * ECHO "Hello, world!"
 * 	 -> string: Hello, world!
 */
public class SelectFrom implements Driver {
	private static final Pattern queryPattern = Pattern.compile(
			
			
			//SELECT\s*(\*|(?:(?:[a-z0-9_]+(?:(?:(?:\s*AS\s*[a-z0-9_]+)*\s*)\s*)?))(?:(?:\s*,\s*[a-z0-9_]+(?:(?:(?:\s*AS\s*[a-z0-9_]+)*\s*)\s*)?))*|[a-z0-9_]+\s*AS\s*[a-z0-9_]+)\s*FROM\s*([a-z0-9_]+)(?:\s*(\s+WHERE\s*([a-z0-9_]+)\s*(<|<>|>|=|>=|<=)\s*([a-z0-9"]+))?)
			
			"SELECT\\s*(\\*|(?:(?:[a-z0-9_]+(?:(?:(?:\\s*AS\\s*[a-z0-9_]+)*\\s*)\\s*)?))(?:(?:\\s*,\\s*[a-z0-9_]+(?:(?:(?:\\s*AS\\s*[a-z0-9_]+)*\\s*)\\s*)?))*|[a-z0-9_]+\\s*AS\\s*[a-z0-9_]+)\\s*FROM\\s*([a-z0-9_]+)(?:\\s*(\\s+WHERE\\s*([a-z0-9_]+)\\s*(<|<>|>|=|>=|<=)\\s*([a-z0-9\"]+))?)",
			Pattern.CASE_INSENSITIVE);

	private static final Pattern literalPattern = InsertInto.literalPattern;
	
	boolean star = false;
	String colNames[];
	String tableName;
	String leftName;
	String rightName;
	String oper;
	String rightValue;
	String where;

	@Override
	public boolean parse(String query) {
		
		Matcher matcher = queryPattern.matcher(query.strip());
		if (!matcher.matches())
			return false;
		
		//sanitize into the following categories
		if(matcher.group(1).equals("*")) {
			star = true;
		}
		//star mode or not
		if(!star) {
		colNames = matcher.group(1).split(",");
		}
		//field for column names or aliases (comma separated list) (split into an array on commas)
		tableName = matcher.group(2);
		
		where = matcher.group(3);
		//field for tableName
		leftName = matcher.group(4);
		//field for LHS column name
		oper = matcher.group(5);
		
		//field for the operator
		rightName = matcher.group(6);
		//field for the RHS literal value (not sanitized)
		
		
		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		
		Table resultSet;
		
		Table table = db.find(tableName);
		if (table == null) {
			throw new QueryError("table is null");
		}
		List<String> columnNames = table.getColumnNames();
		List<FieldType> columnTypes = table.getColumnTypes();
		List<Integer> scales = table.getScales();
		int prime = table.getPrimaryIndex();

		ArrayList<Integer> pointerList = new ArrayList<Integer>();
		// if short form:
		if (star) {

			int counter = 0;
			for (var col : columnNames) {
				pointerList.add(counter);

				counter++;

			}
			
			resultSet = new HashArrayTable( //TODO: update schema based on the requirements
					"_select",
					columnNames,
					columnTypes,
					prime
				);
			
			
		} else {
			int j = 0;
			String instName;
			String instAlias;
			boolean first = false;
			ArrayList<String> resultAlias = new ArrayList<String>();
			ArrayList<FieldType>resultTypes = new ArrayList<FieldType>();
			String[]ind;
			int newprime = prime;
			for (var col : colNames) {
				// System.out.println(col);
				
				col = col.strip();
				ind = col.split(" ");
				
				if(ind.length > 2) {
					instName = ind[0];
					instAlias = ind[2];
				}
				else {
					instName = ind[0];
					instAlias = ind[0];
				}
				
				j = columnNames.indexOf(instName);
				
				if (j == -1) {
//					System.out.println(instName + "hi");
//					System.out.println(columnNames.get(0));
					throw new QueryError("that column does not exist in the schema");
				}
				if (pointerList.contains(j) && resultAlias.contains(instAlias)) {
					throw new QueryError("j is already contained in the pointers list");
				}
				pointerList.add(j);
				resultAlias.add(instAlias);
				resultTypes.add(columnTypes.get(columnNames.indexOf(instName)));
				if(j == prime) {
					if(first == false)
						first = true;
					newprime = pointerList.indexOf(j);
				}
			}
			if (!pointerList.contains(prime)) {
				throw new QueryError("The primary index is not contained in the pointer list");
			}
			
			resultSet = new HashArrayTable( //TODO: update schema based on the requirements
					"_select",
					resultAlias,
					resultTypes,
					newprime
				);
		}
		if(db.exists("_select")) {
			db.drop("_select");
		}
			db.create(resultSet);
		// Phase 1 - find a correspondence between
		// col names in the query and col names in the schema
		// if star form:
				// build the pointers list to exactly match the schema
				//for each index that the pointers list should have
					//add that index as the element to the end of the pointers list
				//also let the schema of the results table be a deep copy of the source table schema
					//with just the name being changed to _select
		// else if long form:
				//initialize lists and other values for a new schema
				// build the pointers list and the result set schema with the correspondence from the query {
				// for each query column name or alias:
				//split the name-or-alias into a separate name and alias
					//split on " " 
					//assign name and alias from split above (if length of array is 1 set alias = name)
		
				// j (real schema index) = index of name in schema
				// if j was -1: throw a query error
				// //if alias is already contained in the column names list: throw a query error
				//if first primary seen, update the primary index
				// add the j index to the end of the pointers list
				// add alias to the column names list we are building
				//add the type to the column types list we are building
				// if the primary index isnt contained in the pointers list: throw a query error
			//let the result set schema be based on the sanitized data above
		
		int lIndex = 0;
		FieldType lType = null;
		FieldType rType = null;
		Object rValue = null;
		
		//Phase 2 (skip if missing where clause)
		if(!Objects.equals(where, null)){
			
			if(columnNames.contains(leftName)) {
			lIndex = columnNames.indexOf(leftName);
			 lType = columnTypes.get(columnNames.indexOf(leftName));
			}
			else {
				throw new QueryError("Left hand column must exist");
			}
			 
			
		
		//update the LHS index to be the corresponding index of the LHS column name in the source tables schema
		//update the LHS type according to the same column you just found
		
		Matcher litMatcher = literalPattern.matcher(rightName);
		
		if (!litMatcher.matches()) {
			throw new QueryError("no matches");
		}

		// if the literal doesnt match any of the available types:
		// throw a query error
		//
		if ((!Objects.equals(litMatcher.group(1), null))) {
			if (litMatcher.group(1).length() > 127) {
				throw new QueryError("The string is too long");
			}
			rValue = litMatcher.group(1);
			rType = STRING;
		}

		else if (!Objects.equals(litMatcher.group(2), null)) {
			rValue = new BigDecimal(litMatcher.group(2));
			rType = DECIMAL;

//			System.out.println(scales.size() + "j");
//			System.out.println(scales);
//			if (scales.get(ncount - 2) != -1) {
//				System.out.println("hi");
//				System.out.println(litMatcher.group(2));
//
//				result = new BigDecimal(litMatcher.group(2)).setScale(scales.get(ncount - 2));
//				System.out.println(result);
//			} else {
//				System.out.println(litMatcher.group(2));
//				result = new BigDecimal(litMatcher.group(2));
//			}
	}
		// if the literal value matches string AND the schema type is also string
		// if the string is too long throw a query error
		// just use your regex with a capture group that doesnt include quotation marks
		// to get the non-quoted chars
		// assign the string as the local variable for the resulting value
		else if (!Objects.isNull(litMatcher.group(3))) {

			try {
				rValue = Integer.parseInt(litMatcher.group(3));
				rType = INTEGER;

			} catch (Exception e) {
				throw new QueryError("Literal cannot be parsed as an int");
			}
			if (litMatcher.group(3).charAt(0) == '0' && litMatcher.group(3).length() > 1) {
				throw new QueryError("Integer can not start with 0");
			}

		}
		// else if the literal value matches integer AND the schema type is also integer
		// parse the literal as an integer (even if it has a sign) if it cant be parsed
		// throw a query error
		// assign the parsed integer as the local variable for the resulting value
		// (Integer Object)

		else if ((!Objects.equals(litMatcher.group(4), null))) {
			try {
				rValue = Boolean.parseBoolean(litMatcher.group(4));
				rType = BOOLEAN;

			} catch (Exception e) {
				throw new QueryError("Literal cannot be parsed as an boolean");
			}
		} else if (Objects.equals(litMatcher.group(5), "null")) {
			rValue = null;
		} else {
			throw new QueryError("This is not cool");
		}
		}
		//Examine the RHS literal:
			//using logic from insert check what type the literal is
			//BUT dont require that the literal type matches the LHS type
			//use the same sanitization steps
			//Update RHS literal type(FieldType) and value(Str, int bool)
		
		
		//Phase 3:
		
		var rows = new ArrayList<>(table.rows());
		
		for (var row : rows) {
			Object lVal = null;
			System.out.println(row.get(prime));
			boolean rowFlag = true;
				if(!Objects.equals(where, null)) {
					lVal = row.get(lIndex);
					if(Objects.equals(lVal, null) || Objects.equals(rValue, null)) {
						rowFlag = false;
						
					}
					else {
						int compnum = 0;
						if(lType.equals(rType)) {
							if(lType.equals(STRING)) {
								compnum = lVal.toString().compareTo(rValue.toString());
								System.out.println("a");
							}
							else if(lType.equals(INTEGER)) {
								//compnum = Integer.compare((Integer)lVal, (Integer)rValue);
								compnum = ((Integer) lVal).compareTo(((Integer)rValue));
								System.out.println(lVal + " " + rValue); 
								System.out.println(compnum);
								System.out.println();
							}
							else if(lType.equals(BOOLEAN)) {
								//compnum = Boolean.compare((Boolean)lVal, (Boolean)rValue);
								compnum = ((Boolean)lVal).compareTo((Boolean)rValue);
								System.out.println((Boolean)lVal);
								System.out.println("lh = %b, rhs = %b, cmp = %d".formatted(lVal,rValue,compnum));
							}
						}
						else {
							compnum = lVal.toString().compareTo(rValue.toString());
							
						}
//						System.out.println(oper);
						if(oper.equals("=")) {
							if(compnum != 0) {
								rowFlag = false;
//								System.out.println("a");
							//System.out.println("lh = %b, rhs = %b, cmp = %d, flag = %b".formatted(lVal,rValue,compnum,rowFlag));
							}
						}
							else if(oper.equals("<>")) {
								if(compnum == 0) {
									rowFlag = false;
								}
							}
							else if(oper.equals("<")) {
								if(compnum >= 0) {
									rowFlag = false;
								}
							}
							else if(oper.equals(">")) {
								if(compnum <= 0) {
									rowFlag = false;
								}
							}
							else if(oper.equals("<=")) {
								if(compnum > 0) {
									rowFlag = false;
								}
							}
							else if(oper.equals(">=")) {
								if(compnum < 0) {
									rowFlag = false;
								}
							}
						
							
						}
					}
				
				if(rowFlag) {
					ArrayList<Object> selectedRow = new ArrayList<Object>();
					for(var j : pointerList) {
						selectedRow.add(row.get(j));
					}
					resultSet.put(selectedRow);
				}
				
				}
		
		
			//for each row in the source table (source row)
				//initialize a selection flag (whether the row should be selected) to true by default
				//if there is an operator(where clause)
		
					//get the LHS value from the source row
					
					//if special case for null: whenever LHS or RHS is null dont select the row
											//	whenever LHS or RHS is null, the operator evaluates to false (no selection occurs)
											//	if detected, set the selection flag immediately to false
											//  skip the rest of the operators/ logic (even the build portion)
		
					//else 
		
						//define a variable to contain the comparison number
		
						//if LHS and RHS value are the same type
							//if both are strings
								//compare them as strings, get the comparison number
				
							//if both are integers
								//compare them as integers, get the comparison number
		
							//if both are booleans
								//compare them as booleans and get the comparison number
		
						//else if they are different types
							//convert them both to strings
							//compare them as strings, get the comparison number
		
		
						//if the operator is "="
							//select flag updates to be whether the comp number is 0
		
							//select = (condition on the comparison number)
		
						//else if the operator is "<>"
							//select flag updates to be whether comp number != 0
		
			//If selected build and add the row
				//make a new empty row(selected row)
				//for each j index in pointers
					//get the field from the source row at index j
					//add it on to the new selected row that we are building
				//put the new selected row in the results set
		
		//return the results set (never make a change to the source table)
		
		
		
		return resultSet;
	}
}
