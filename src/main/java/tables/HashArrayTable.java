package tables;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import sql.FieldType;

/**
 * Implements a hash-based table using an array data structure.
 */
public class HashArrayTable extends Table {
	private Object[] array;
	private int size;
	private int contamination; // OA
	private int fingerprint;

	private static final int MIN_CAPACITY = 7; // prime number < 20
	private static final double LOAD_FACTOR_BOUND = .75; // OA

	private static final Object TOMBSTONE = new Object(); // OA

	/**
	 * Creates a table and initializes the data structure.
	 *
	 * @param tableName    the table name
	 * @param columnNames  the column names
	 * @param columnTypes  the column types
	 * @param primaryIndex the primary index
	 */
	public HashArrayTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		clear();
	}

	@Override
	public void clear() {

		array = new Object[MIN_CAPACITY];
		size = 0;
		contamination = 0; // OA
		fingerprint = 0;

	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);

		Object key = row.get(getPrimaryIndex());
		final int hash = hashFunction(key);

		int t = -1;

		for (int j = 0; j < array.length; j++) // OA
		{
			int i = wrap(hash + j * j * (j % 2 == 0 ? +1 : -1)); // ASQP

			if (array[i] == TOMBSTONE) {
				if (t == -1) {
					t = i;
				}

				// if t has never been assigned an index
				// then update t to match
				continue;
			}

			if (array[i] == null) { // miss
				if (t == -1) {
					array[i] = row;
					size++;
					// fingerprint
					fingerprint += row.hashCode();
				}

				// if t has never been assigned an index
				// store the row at position i
				// adjust the metadata
				// size increases by 1
				// fingerprint increases
				else {
					array[t] = row;
					size++;
					contamination--;
					fingerprint += row.hashCode();
					// fingerprint
				}

				// otherwise, t is a recycling location
				// store the row at position t
				// adjust the metadata
				// size increases by 1
				// contamination decreases by 1
				// fingerprint increases

				// if necessary, rehash
				if (loadFactor() > LOAD_FACTOR_BOUND) {
					rehash();
				}
				return false;
			}

			List<Object> old = (List<Object>) array[i];
			if (old.get(getPrimaryIndex()).equals(key)) {

				if (t == -1) {
					//fingerprint -= array[i].hashCode();
					array[i] = row;
					fingerprint += row.hashCode() - old.hashCode();
					// fingerprint
					return true;
				} else {
					//fingerprint -= array[i].hashCode();
					array[t] = row;
					array[i] = TOMBSTONE;
					fingerprint += row.hashCode() - old.hashCode();
					
					// fingerprint
				}
				// hit
				// if t has never been assigned an index
				// update the row at position i with its replacement
				// adjust the metadata
				// fingerprint increases/decreases
				// return true

				// otherwise, t is a recycling location
				// replace the tombstone at position t with the row
				// put a tombstone at position i
				// adjust the metadata
				// fingerprint increases/decreases

				return true;
			}
			// not yet a hit or miss, so continue
		}

		//throw new IllegalStateException();
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object key) {
		
		//List<Object>row = ((List<Object>) array[key]).remove(key);
		final int hash = hashFunction(key);

		for (int j = 0; j < array.length; j++) // OA
		{
			int i = wrap(hash + j * j * (j % 2 == 0 ? +1 : -1)); // ASQP

			if (array[i] == TOMBSTONE) {

				continue;
			}

			if (array[i] == null) { // miss
				return false;
			}
			
		

			List<Object> old = (List<Object>) array[i];
			if (old.get(getPrimaryIndex()).equals(key)) {
				
				array[i] = TOMBSTONE;
				size--;
				fingerprint -= old.hashCode();

				return true;
			}
			
		}

		
		return false;
	}

	@Override
	public List<Object> get(Object key) {

		final int hash = hashFunction(key);

		for (int j = 0; j < array.length; j++) // OA
		{
			int i = wrap(hash + j * j * (j % 2 == 0 ? +1 : -1)); // ASQP

			if (array[i] == TOMBSTONE) {

				continue;
			}

			if (array[i] == null) { // miss
				return null;
			}

			List<Object> old = (List<Object>) array[i];
			if (old.get(getPrimaryIndex()).equals(key)) { // hit
				return old;
			}
		}
		// not yet a hit or miss, so continue

		// throw new IllegalStateException();
		// return false;
		return null;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public int capacity() {
		return array.length;
	}

	@Override
	public double loadFactor() { // OA
		return (double) (size + contamination) / (double) array.length;
	}

	public void rehash() {
		
		Object[] backup = array;
		array = new Object[nextPrime(array.length)];
		size = 0;
		contamination = 0;
		fingerprint = 0;
		for(int a = 0; a < backup.length; a++) {
			if(backup[a] != TOMBSTONE && backup[a] != null) {
			put((List<Object>)backup[a]);
			}
		}
		// let backup reference point to the existing array

		// reassign array reference to point to
		// a new array which is roughly twice as large
		// but is still a valid prime number (based on CRT)
		// by find nextPrime from current capacity
		// ex. nextPrime(7) -> 19
		// or nextPrime(array.length) -> new length

		// reset all metadata

		// for each row (not a null or tombstone) in the table
		// call put with that row as the parameter
	}

	private int nextPrime(int prev) {
		int next = (prev * 2) + 1;
		
		if((next % 4) != 3) {
			next = next + 2;
		}
			while(!isPrime(next)) {
				next = next + 4;
			}
		
		
		return next;
		// let next be twice the prev plus 1

		// ASQP:
		// if it is not the case that next modulo 4 is congruent to 3
		// step up next by 2

		// while its not the case that next isPrime:
		// step next up by 4

		// return next
	}

	private boolean isPrime(int number) {
		double root = Math.sqrt(number);
		for(int n = 3; n <= root; n = n + 2) {
			if(number % n == 0) {
				return false;
			}
		}
		// for each factor of the number
		// from 3
		// up to and including the square root (compute before loop)
		// stepping up by 2 each time
		// body:
		// if number is divisible by the factor:
		// return false
		//
		return true;
	}

	// if double hashing copy and paste method and change name, use different salt

	private static int hashFunction(Object key) {
		String input = "%s-%s-%s".formatted("Glen", // salt
				key.hashCode(), key.toString());

		// Polynomial rolling hash
		// char[]chars = input.toCharArray();
		// int hash = 0;
		// implement PRH using chars array

		// FNV hash
		byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
		int hash = 0;
		int hashInit = 0x811c9dc5;
		int hashPrime = 16777619;
		for (int a = 0; a < bytes.length; a++) {
			hash = hashInit * hashPrime;
			hash = hash ^ bytes[a];
		}

		// add to the hash according to the algorithm

		return hash;
	}

	private int wrap(int index) {
		// index % array.length
		return Math.floorMod(index, array.length);
	}

	@Override
	public int hashCode() {
		return fingerprint;
	}

	//for(initialization; maintenance condition; incrementation){
	//body
	//}
	
	@Override
	public Iterator<List<Object>> iterator() {
		return new Iterator<>() {
			//initialize loop control
			int index = skip(0);
			private int skip(int num){
				while(num < array.length) {
					if(array[num] == null || array[num] == TOMBSTONE) {
						num++;
					}
					else {
						return num;
					}
					
				}
				return array.length;
			}
			@Override
			public boolean hasNext() { //maintenance condition
				// answer the question
				//not do any mutations
				if(index < array.length && array[index] != null && array[index] != TOMBSTONE) {
					return true;
				}
				else {
				return false;
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			public List<Object> next() { //body, incrementation
				int temp = index;
				index = skip(index +1);
				// handle all mutations
				//answer the question
				//System.out.println((List<Object>)array[index]);
				return (List<Object>) array[temp];
				
				
			}
			
		};
		/* return new Iterator<>() {
			int index = 0;

			@Override
			public boolean hasNext() {
				return index < list.size();
			}

			@Override
			public List<Object> next() {
				return list.get(index++);
			}
		};
		*/
	}
}
