import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.regex.*;

public class Assignment {

  public static void main(String args[]) throws ClassNotFoundException {
    Assignment.CSVtoDatabase();
    Assignment.DatabasetoCSV();
    
  }


    public static void CSVtoDatabase() throws ClassNotFoundException{
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root",
                    "maveric123");
            // connection.setAutoCommit(false);
            String sql = "INSERT INTO employee (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER ,HIRE_DATE ,JOB_ID, SALARY,COMMISSION_PCT,MANAGER_ID,DEPARTMENT_ID,CREATED_DATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String sqlfail = "INSERT INTO employee_failed (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER ,HIRE_DATE ,JOB_ID, SALARY,COMMISSION_PCT,MANAGER_ID,DEPARTMENT_ID,CREATED_DATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement statement = connection.prepareStatement(sql);
            PreparedStatement statementfail = connection.prepareStatement(sqlfail);

            BufferedReader lineReader = new BufferedReader(
                    new FileReader("C:\\Users\\sunil\\Downloads\\employee.csv"));
            String lineText = null;
            lineReader.readLine();

            while ((lineText = lineReader.readLine()) != null) {
                boolean fail = false;
                String[] data = lineText.split(",");
                if (data.length > 0) {       
                    Integer EMPLOYEE_ID = Integer.parseInt(data[0]);
                    String FIRST_NAME = data[1];
                    String LAST_NAME;
                    String LAST_NAME_fail = "";
                    if (data[2].isEmpty()) {
                        fail = true;
                        LAST_NAME = "-";
                        LAST_NAME_fail = data[2];
                    } else {
                        LAST_NAME = data[2];
                    }

                    String EMAIL;
                    String EMAIL_fail = " ";
                    String regex = "^(.+)@(.+)$";
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(data[3]);
                    if (matcher.matches()) {
                        EMAIL = data[3];
                    } else {
                        fail = true;
                        EMAIL = "-";
                        EMAIL_fail = data[3];
                    }

                    String PHONE_NUMBER;
                    String PHONE_NUMBER_fail = "";
                    if (data[4].matches("\\d{3}[-\\.\\s]\\d{3}[-\\.\\s]\\d{4}")) {
                        PHONE_NUMBER = data[4];
                    } else {
                        fail = true;
                        PHONE_NUMBER = "-";
                        PHONE_NUMBER_fail = data[4];
                    }

                    String HIRE_DATE;
                    String HIRE_DATE_fail = "";
                    if (data[5].matches("\\d{2}-[a-zA-Z0-9]{3}-\\d{2}|d{4}")) {
                        HIRE_DATE = data[5];
                    } else {
                        fail = true;
                        HIRE_DATE = "-";
                        HIRE_DATE_fail = data[5];
                    }

                    String JOB_ID = data[6];

                    String SALARY;
                    String SALARY_fail = "";
                    if ((data[7].equals("" + Integer.parseInt(data[7]))) && (0 < Integer.parseInt(data[7]))) {
                        SALARY = data[7];
                    } else {
                        fail = true;
                        SALARY = "-";
                        SALARY_fail = data[7];
                    }

                    String COMMISSION_PCT = data[8];
                    String MANAGER_ID = data[9];
                    String DEPARTMENT_ID = data[10];

                    statement.setInt(1, EMPLOYEE_ID);
                    statement.setString(2, FIRST_NAME);
                    statement.setString(3, LAST_NAME);
                    statement.setString(4, EMAIL);
                    statement.setString(5, PHONE_NUMBER);
                    statement.setString(6, HIRE_DATE);
                    statement.setString(7, JOB_ID);
                    statement.setString(8, SALARY);
                    statement.setString(9, COMMISSION_PCT);
                    statement.setString(10, MANAGER_ID);
                    statement.setString(11, DEPARTMENT_ID);
                    statement.setTimestamp(12, Timestamp.valueOf(LocalDateTime.now()));
                    statement.executeUpdate();

                    if (fail) {
                        statementfail.setInt(1, EMPLOYEE_ID);
                        statementfail.setString(2, FIRST_NAME);
                        statementfail.setString(3, LAST_NAME_fail);
                        statementfail.setString(4, EMAIL_fail);
                        statementfail.setString(5, PHONE_NUMBER_fail);
                        statementfail.setString(6, HIRE_DATE_fail);
                        statementfail.setString(7, JOB_ID);
                        statementfail.setString(8, SALARY_fail);
                        statementfail.setString(9, COMMISSION_PCT);
                        statementfail.setString(10, MANAGER_ID);
                        statementfail.setString(11, DEPARTMENT_ID);
                        statementfail.setTimestamp(12, Timestamp.valueOf(LocalDateTime.now()));
                        statementfail.executeUpdate();
                    }
                }
            }
            lineReader.close();
            connection.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }catch (Exception ex) {
            ex.printStackTrace();
        }

    }


    public static void DatabasetoCSV() throws ClassNotFoundException{
        try {

            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root",
                    "maveric123");

            Statement stmt = connection.createStatement();
            Statement stmt1 = connection.createStatement();

            String success = "SELECT employee.EMPLOYEE_ID , employee.FIRST_NAME, employee.EMAIL, employee_job.Job_description, Manager.MANAGER_NAME, departments.DEPARTMENT_NAME FROM employee INNER JOIN employee_job ON employee.JOB_ID = employee_job.JOB_ID INNER JOIN departments ON employee.DEPARTMENT_ID = departments.DEPARTMENT_ID INNER JOIN Manager ON employee.MANAGER_ID = Manager.MANAGER_ID;";
            String failed = "SELECT employee_failed.EMPLOYEE_ID , employee_failed.FIRST_NAME, employee_failed.EMAIL, employee_job.Job_description, Manager.MANAGER_NAME, departments.DEPARTMENT_NAME FROM employee_failed INNER JOIN employee_job ON employee_failed.JOB_ID = employee_job.JOB_ID INNER JOIN departments ON employee_failed.DEPARTMENT_ID = departments.DEPARTMENT_ID INNER JOIN Manager ON employee_failed.MANAGER_ID = Manager.MANAGER_ID;";

            ResultSet result = stmt.executeQuery(success);
            ResultSet result1 = stmt1.executeQuery(failed);

            BufferedWriter fileWriter = new BufferedWriter(
                    new FileWriter("C:\\Users\\sunil\\Downloads\\successemployee.csv"));
            BufferedWriter fileWriter1 = new BufferedWriter(
                    new FileWriter("C:\\Users\\sunil\\Downloads\\failedemployee.csv"));

            // write header line containing column names       
            fileWriter.write("EMPLOYEE_ID,FIRST_NAME,EMAIL,Job_description,MANAGER_NAME,DEPARTMENT_NAME");
            fileWriter1.write("EMPLOYEE_ID,FIRST_NAME,EMAIL,Job_description,MANAGER_NAME,DEPARTMENT_NAME");

            while (result.next()) {
                String EMPLOYEE_ID = result.getString("EMPLOYEE_ID");
                String FIRST_NAME = result.getString("FIRST_NAME");
                String EMAIL = result.getString("EMAIL");
                String Job_description = result.getString("Job_description");
                String MANAGER_NAME = result.getString("MANAGER_NAME");
                String DEPARTMENT_NAME = result.getString("DEPARTMENT_NAME");

                String line = String.format("%s,%s,%s,%s,%s,%s",
                        EMPLOYEE_ID, FIRST_NAME, EMAIL, Job_description, MANAGER_NAME, DEPARTMENT_NAME);

                fileWriter.newLine();
                fileWriter.write(line);
            }
            while (result1.next()) {
                String EMPLOYEE_ID = result1.getString("EMPLOYEE_ID");
                String FIRST_NAME = result1.getString("FIRST_NAME");
                String EMAIL = result1.getString("EMAIL");
                String Job_description = result1.getString("Job_description");
                String MANAGER_NAME = result1.getString("MANAGER_NAME");
                String DEPARTMENT_NAME = result1.getString("DEPARTMENT_NAME");

                String line = String.format("%s,%s,%s,%s,%s,%s",
                        EMPLOYEE_ID, FIRST_NAME, EMAIL, Job_description, MANAGER_NAME, DEPARTMENT_NAME);

                fileWriter1.newLine();
                fileWriter1.write(line);
            }

           
            stmt.close();
            stmt1.close();
            fileWriter.close();
            fileWriter1.close();

        } catch (IOException ex) {
            ex.printStackTrace();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }
}