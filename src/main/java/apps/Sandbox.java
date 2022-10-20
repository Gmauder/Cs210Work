package apps;

import static sql.FieldType.*;

import java.util.Arrays;
import java.util.List;

import tables.SearchTable;
import tables.Table;

/**
 * Sandbox for execution of arbitrary code
 * for testing or grading purposes.
 * <p>
 * Modify the code for your use case.
 */
@Deprecated
public class Sandbox {
	public static void main(String[] args) {
		Table table = new SearchTable(
			"sandbox_1",
			List.of("letter", "order", "vowel"),
			List.of(STRING, INTEGER, BOOLEAN),
			1
		);

		table.put(List.of("alpha", 1, true));
		table.put(List.of("beta", 26, false));
		table.put(List.of("gamma", 3, false));
		table.put(List.of("delta", 34, false));
		table.put(List.of("tau", 19, false));
		table.put(List.of("pi", 2, false));
		table.put(List.of("omega", 24, true));
		table.put(Arrays.asList("N/A", 6, null));

		System.out.println(table);
		
		
		Table table2 = new SearchTable(
				"sandbox_2",
				List.of("letter", "order", "vowel"),
				List.of(STRING, INTEGER, BOOLEAN),
				0
			);

			table2.put(List.of("alphaxxxxxxxxxxx", 1, true));
			table2.put(List.of("beta", 2, false));
			table2.put(List.of("gamma", 3, false));
			table2.put(List.of("delta", 4, false));
			table2.put(List.of("tau", 19, false));
			table2.put(List.of("pi", 16, false));
			table2.put(List.of("omega", 24, true));
			table2.put(Arrays.asList("N/A", null, null));

			System.out.println(table2);
			
			
			Table table3 = new SearchTable(
					"sandbox_3",
					List.of("letter", "order", "vowel", "input"),
					List.of(STRING, INTEGER, BOOLEAN, BOOLEAN),
					0
				);

				table3.put(List.of("alpha", 1, true, true));
				table3.put(List.of("beta", 2, false, true));
				table3.put(List.of("gamma", 3, false, true));
				table3.put(List.of("delta", 4, false, true));
				table3.put(List.of("tau", 19, false, false));
				table3.put(List.of("pi", 16, false, false));
				table3.put(List.of("omega", 24, true, false));
				table3.put(Arrays.asList("N/A", null, null, false));

				System.out.println(table3);
				
				Table table4 = new SearchTable(
						"sandbox_2",
						List.of("letter", "order"),
						List.of(STRING, INTEGER),
						0
					);

					table4.put(List.of("alphaxxxxxxxxxxx", 1));
					table4.put(List.of("beta", 2));
					table4.put(List.of("gamma", 3));
					table4.put(List.of("delta", 4));
					table4.put(List.of("tau", 19));
					table4.put(List.of("pi", 16));
					table4.put(List.of("omega", 24));
					table4.put(Arrays.asList("N/A", null));

					System.out.println(table4);
			
	}
	
	
}
