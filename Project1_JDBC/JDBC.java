import java.sql.*;

public class JDBC {
    private static final int NO_ROWS_AFFECTED = 0;
    private final Connection connection;
    private String[] colHeaders;
    private String[] rowData;

    public JDBC(String connectionUrl) throws SQLException {
        connection = DriverManager.getConnection(connectionUrl);
    }

    public void SqlQueryUpdate(String sqlQuery) throws SQLException {
        int rowCount = connection.createStatement().executeUpdate(sqlQuery);

        if (rowCount == NO_ROWS_AFFECTED) {
            System.out.println("No Update");
        } else {
            System.out.printf("(%d row(s) affected)%n", rowCount);
        }
    }

    public String SqlQueryLookUp(String sqlQuery) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery(sqlQuery);
        StringBuilder result = new StringBuilder();
        int columnCount = rs.getMetaData().getColumnCount();
        int[] columnSizePrint = new int[columnCount + 1];

        //System.out.print("|");
        result.append("| ");
        for (int i = 1; i <= columnCount; i++) {
            columnSizePrint[i] = rs.getMetaData().getColumnDisplaySize(i);
            String columnName = rs.getMetaData().getColumnName(i);

            //System.out.printf("%-" + (columnSizePrint[i] + 1) + "s| ", columnName);
            result.append(String.format("%-" + (columnSizePrint[i] + 1) + "s| ", columnName)).append(" |");
        }
        result.append("\n");

        while (rs.next()) {
            //System.out.print("|");
            result.append("| ");
            for (int i = 1; i <= columnCount; i++) {
                String value = rs.getString(i);
                //System.out.printf("%-" + (columnSizePrint[i] + 1) + "s| ", value);
                result.append(String.format("%-" + (columnSizePrint[i] + 1) + "s| ", value));
            }
            result.append("\n");
        }
        result.append("\n");
        rs.close();
        return result.toString();
    }

    /**
     * Executes the query and adds data to result
     * Within ColumnHeaders to describe the column
     * Within rowData that tells is what is the data where the components are seperated
     * by the delimiter *_*
     * @implNote The reason for using <code> ResultSet.TYPE_SCROLL_INSENSITIVE</code> is to use it to skip
     *           to the first row once data is done processing.
     */
    public void SqlQueryLookUpStoreLookup(String sqlQuery) throws SQLException {
        ResultSet rs = connection
                .createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
                .executeQuery(sqlQuery);

        storeColumnHeaders(rs.getMetaData());

        int rowCount = getRowCount(rs);

        storeRows(rowCount, rs);

        rs.close();
    }

    /**
     * Iterates through the rows and adds the delimeter for faster processing
     * That can be split for later use
     * @param rowCount The amount of rows to create the array
     * @param rs The incoming result set that assumes it is above
     *           the 1st row
     * @implNote The reason for using a delimeter is to save space
     *           We could have easily made rows a 2d matrix but this is very inefficient
     **/
    private void storeRows(int rowCount, ResultSet rs) throws SQLException {
        rowData = new String[rowCount];
        int columnCount = rs.getMetaData().getColumnCount();
        String delimiter = "*_*";
        int idx = 0;

        while (rs.next()) {
            StringBuilder rowStr = new StringBuilder();

            for (int i = 1; i <= columnCount; i++) {
                String value = rs.getString(i);
                rowStr.append(value).append(i == columnCount ? "" : delimiter);
            }

            rowData[idx] = rowStr.toString();
            idx++;
        }

    }

    /**
     * Stores the columns for the caller to be able to render the table properly
     * @param metaData holds the column information
     **/
    private void storeColumnHeaders(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();
        colHeaders = new String[columnCount];

        for (int i = 1; i <= columnCount; i++) {
            colHeaders[i-1] = metaData.getColumnName(i);
        }

    }

    /**
     * Gets the row count and positions it back to the beginning
     */
    public int getRowCount(ResultSet resultSet){
        int rowCount = 0;
        try {
            boolean atFirstRow = resultSet.isBeforeFirst();

            while (resultSet.next()) {
                rowCount++;
            }

            if (!atFirstRow) {
                resultSet.beforeFirst();
            }

            resultSet.beforeFirst();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return rowCount;
    }

    public String[] getColHeaders() {
        return colHeaders;
    }

    public String[] getRowData() {
        return rowData;
    }

    }