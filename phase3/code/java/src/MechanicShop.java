/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		String first_name;

		do {    
        		System.out.print("Enter First name: ");
       		 	try {   
                		first_name = in.readLine(); 
                		if(first_name.length() <= 0 || first_name.length() > 32) {
                			throw new RuntimeException("First name cannot be null or exceed 32 characters");
        			}break;
        
        		}catch (Exception e) {
        			System.out.println (e);
        			continue;
        		}
		}while (true);

		String last_name;

		do {    
        		System.out.print("Enter Last name: ");
        		try {   
                		last_name = in.readLine(); 
                		if(last_name.length() <= 0 || last_name.length() > 32) {
                			throw new RuntimeException("Last name cannot be null or exceed 32 characters");
        			}break;
        
        		}catch (Exception e) {
        			System.out.println(e);
        			continue;
        		}
		}while (true);

		int ID;

        	do {
                	System.out.print("Enter Customer ID: ");
        		try {
                		ID = Integer.parseInt(in.readLine());
                		if(ID <= 0) throw new RuntimeException("Customer ID cannot be null");
        			break;
        		}catch (Exception e) {
                		System.out.println(e);
                		continue;
        		}
		}while (true);

		String phone_num;

		do {    
        		System.out.print("Enter Phone number: ");
        		try {   
                		phone_num = in.readLine(); 
                		if(phone_num.length() <= 0 || phone_num.length() > 13) {
                			throw new RuntimeException("Phone number cannot be null or exceed 13 characters");
        			}break;
        
        		}catch (Exception e) {
        			System.out.println(e);
        			continue;
        		}
		}while (true);

		String address;

		do {
        		System.out.print("Enter Address: ");
        		try {
                		address = in.readLine();
                		if(address.length() <= 0 || address.length() > 256) {
                			throw new RuntimeException("Last name cannot be null or exceed 256 characters");

        			}
				break;
        		}catch (Exception e) {
        			System.out.println(e);
        			continue;
        		}
		}while (true);

		try {
      			String query = "INSERT INTO Customer (id, fname, lname, phone, address) VALUES (" + ID + ", \'" + first_name + "\', \'" + last_name + "\',  \'" + phone_num  + "\',  \'" + address + "\' );";

                        esql.executeUpdate(query);
                }catch (Exception e) {
                        System.err.println (e.getMessage());
                }
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		int ID;

	        do {
                	System.out.print("Enter Employee ID: ");
                	try {
                        	ID = Integer.parseInt(in.readLine());
                        	if(ID <= 0) throw new RuntimeException("Employee ID cannot be null");
                        	break;
                	}catch (Exception e) {
                        	System.out.println(e);
                        	continue;
                	}
        	}while (true);


        	String first_name;

        	do {
                	System.out.print("Enter First name: ");
                	try {
                        	first_name = in.readLine();
                        	if(first_name.length() <= 0 || first_name.length() > 32) {
                                	throw new RuntimeException("First name cannot be null or exceed 32 characters");
                        	}break;

                	}catch (Exception e) {
                        	System.out.println (e);
                        	continue;
                	}
        	}while (true);

        	String last_name;

        	do {
                	System.out.print("Enter Last name: ");
                	try {
                        	last_name = in.readLine();
                        	if(last_name.length() <= 0 || last_name.length() > 32) {
                        		throw new RuntimeException("Last name cannot be null or exceed 32 characters");
                		}break;

                	}catch (Exception e) {
                        	System.out.println(e);
                        	continue;
                	}
        	}while (true);

        	int experience;
        
        	do{
                	System.out.print("Enter employee's experience(number of years): "
                	try{
            
                        	experience = Integer.parseInt(in.readLine());
                        	if(experience < 0 || experience >= 100) throw new RuntimeException("Employee's experience cannot be null or exceed 100 years");
                        	break;
                	}catch (Exception e) {
                        	System.out.println(e);
                        	continue;       
                	}               
        	}while (true);          
                        
        	try {                   
                	String query = "INSERT INTO Mechanic (id, fname, lname, experience) VALUES (" + ID + ", \'" + first_name + "\', \'" + last_name + "\',  " + experience  + " );";
                        
                        esql.executeUpdate(query);
        	}catch (Exception e) {
                	System.err.println (e.getMessage());
        	}
	}
	
	public static void AddCar(MechanicShop esql){//3
		String in1;
                do{
                        System.out.print("\tEnter VIN: ");
                        try{
                                in1 = in.readLine();
                                if(in1.length() <= 0 || in1.length() > 16) {
                                        throw new RuntimeException("VIN cannot be null or exceed 16 characters");
                                }
                                break;
                        } catch(Exception e) {
                                System.out.println("Your input is invalid!");
                                continue;
                        }
                } while(true);

                String in2;
                do{
                        System.out.print("\tEnter make: ");
                        try{
                                in2 = in.readLine();
                                if(in2.length() <=0 || in2.length() > 32) {
                                        throw new RuntimeException("Make cannot be null or exceed 32 characters");
                                }
                                break;
                        } catch(Exception e) {
                                System.out.println("Your input is invalid!");
                                continue;
                        }
                } while(true);

                String in3;
                do{
                        System.out.print("\tEnter model: ");
                        try{
                                in3 = in.readLine();
                                if(in3.length() <= 0 || in3.length() > 32) {
                                        throw new RuntimeException("Model cannot be null or exceed 32 characters");
                                }
                                break;
                        } catch(Exception e) {
                                System.out.println("Your input is invalid!");
                                continue;
                        }
                } while(true);

 		int in4;
                do{
                        System.out.print("\tEnter year: ");
                        try{
                                in4 = Integer.parseInt(in.readLine());
                                if(in4 < 1970) {
                                        throw new RuntimeException("Year cannot be less than 1970");
                                }
                                break;
                        } catch(Exception e) {
                                System.out.println("Your input is invalid!");
                                continue;
                        }
                } while(true);

                try{
                        String query = "INSERT INTO Car(vin, make, model, year) VALUES(\'" + in1 + "\',\'" + in2 + "\',\'" + in3 + "\'," + in4 +")";
                        esql.executeUpdate(query);
                } catch(Exception e) {
                        System.err.println(e.getMessage());
                }

		try{
			String query2 = "SELECT * FROM Car WHERE vin = ";
			query2 += in1;

			esql.executeQuery(query2);

	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try{
                        String query = "SELECT c.fname AS FirstName, c.lname AS LastName, b.bill FROM Customer c, Service_Request a, Closed_Request b WHERE c.id = a.customer_id AND a.rid = b.rid AND b.bill < 100;";
                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println("total row(s): " + rowCount);
                }
                catch(Exception e){
                        System.err.println(e.getMessage());
                }

	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try{
			String query = "SELECT fname, lname FROM Customer WHERE id IN (SELECT customer_id FROM Owns GROUP BY customer_id HAVING COUNT(customer_id) > 20)";
			
			int rowCount = esql.executeQuery(query);
			System.out.println("total row(s): " + rowCount);

			esql.executeQueryAndPrintResult(query);
		} catch(Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try{
                        String query = "SELECT DISTINCT make, model, year FROM Car AS C, Service_Request AS S WHERE year < 1995 and S.car_vin = C.vin and S.odometer < 50000;";
                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println("total row(s): " + rowCount);
                }
                catch(Exception e){
                        System.err.println(e.getMessage());
                }
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		 try{
                        String query = "SELECT make, model, a.num_requests FROM Car c, (SELECT car_vin, COUNT(rid) AS num_requests FROM Service_Request GROUP BY car_vin ) AS a WHERE a.car_vin = c.vin ORDER BY a.num_requests DESC LIMIT "    ;
                        System.out.println("Enter the number of cars you want to view: ");
                        String num = in.readLine();
                        query += num + ";";

                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println("total row(s): " + rowCount);
                }
                catch(Exception e){
                        System.err.println(e.getMessage());
                }

	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//9
		try{
                        String query = "SELECT C.fname, C.lname, total FROM Customer AS C,(SELECT SR.customer_id, SUM(CR.bill) AS total FROM Closed_Request AS CR, Service_Request AS SR WHERE CR.rid = SR.rid GROUP BY SR.customer_id) AS B WHERE C.id=B.customer_id ORDER BY B.total DESC;";
                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println("total row(s): " + rowCount);
                }
                catch(Exception e){
                        System.err.println(e.getMessage());
                }	
	}
	
}
