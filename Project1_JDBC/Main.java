import java.sql.SQLException;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        try {
            Map<String, String> env = System.getenv();
            String port = env.get("CONTAINER_PORT");
            String password = env.get("CONTAINER_PASSWORD");

            String connectionUrl = String.format("jdbc:sqlserver://localhost:%s;" +
                    "databaseName=master;" +
                    "encrypt=true;" +
                    "trustServerCertificate=true;" +
                    "user=sa;" +
                    "password=%s", port, password);

            JDBC myDatabase = new JDBC(connectionUrl);
            String sqlQueryLookup =
                    """         
                                 
                    """;

            String sqlQueryUpdate =
                    """

                    """;
            GUI example = new GUI();
            example.setVisible(true);

            //myDatabase.SqlQueryLookUp(sqlQueryLookup);
            //myDatabase.SqlQueryUpdate(sqlQueryLookup);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
