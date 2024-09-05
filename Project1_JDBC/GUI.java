import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.sql.SQLException;
import java.util.Map;
import java.util.Vector;

public class GUI extends JFrame {

    private final JTextArea inputTextArea;
    private final DefaultTableModel tableModel;

    private JDBC myDatabase;

    public GUI() throws SQLException {
        Map<String, String> env = System.getenv();
        String domain = env.get("DOMAIN");
        String port = env.get("PORT");
        String userName = env.get("USERNAME");
        String password = env.get("PASSWORD");

        String connectionUrl = String.format("jdbc:sqlserver://%s:%s;" +
                "databaseName=master;" +
                "encrypt=true;" +
                "trustServerCertificate=true;" +
                "user=%s;" +
                "password=%s", domain, port, userName, password);
        connectDB(connectionUrl);

        setTitle("JDBC GUI");
        setSize(1200, 800);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        // Create input text area
        inputTextArea = new JTextArea();
        JScrollPane inputScrollPane = new JScrollPane(inputTextArea);


        tableModel = new DefaultTableModel();
        JTable outputTable = new JTable(tableModel);
        outputTable.setEnabled(false);

        JScrollPane outputScrollPane = new JScrollPane(outputTable);
        outputScrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);

        JButton executeButton = executeQueryButton();

        JPanel textPanel = new JPanel(new GridLayout(1, 2));
        textPanel.add(inputScrollPane);
        textPanel.add(outputScrollPane);

        // Add components to the frame
        getContentPane().add(textPanel, BorderLayout.CENTER);
        getContentPane().add(executeButton, BorderLayout.SOUTH);

        // Create menu bar
        JMenuBar menuBar = new JMenuBar();
        JMenu fileMenu = new JMenu("Tools");
        JMenuItem clearMenuItem = new JMenuItem("Clear Table");
        clearMenuItem.addActionListener(e -> clearTable());

        JMenuItem loadSchemaMenuItem = new JMenuItem("Load Schema");
        loadSchemaMenuItem.addActionListener(e -> inputTextArea.setText("EXEC [G9_1_BIClass].[Project2].[LoadStarSchemaData]"));

        JMenuItem workFlowStepsMenuItem = new JMenuItem("Show Workflow");
        workFlowStepsMenuItem.addActionListener(e -> inputTextArea.setText("EXEC [G9_1_BIClass].Process.usp_ShowWorkflowSteps _"));

        fileMenu.add(clearMenuItem);
        fileMenu.add(loadSchemaMenuItem);
        fileMenu.add(workFlowStepsMenuItem);

        menuBar.add(fileMenu);
        setJMenuBar(menuBar);
    }


    private void clearTable() {
        tableModel.setRowCount(0);
        tableModel.setColumnIdentifiers((Vector<?>) null);
    }

    private JButton executeQueryButton() {
        JButton executeButton = new JButton("Execute Query");
        executeButton.addActionListener(e -> {
            try {
                executeAndStore();
                renderTable();
            } catch (SQLException ex) {
                JOptionPane.showMessageDialog(this, "Error executing SQL: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                ex.printStackTrace();
            }
        });
        return executeButton;
    }

    /**
     * Clears the Table and gets the new Result set
     *  (Table ready to be shown). It uses side effects
     *  from JDBC so the order in which this method is called
     *  is important.
     **/
    private void renderTable() {
        clearTable();

        String[] colHeaders = myDatabase.getColHeaders();
        String[] rowData = myDatabase.getRowData();

        tableModel.setColumnIdentifiers(colHeaders);

        for (String rowDatum : rowData) {
            String[] rowSplit = rowDatum.split("\\*\\_\\*");
            tableModel.addRow(rowSplit);
        }

    }

    /**
     * Executes the query if possible and
     * Stores the result set in two variables.
     *
     *  It uses side effects
     *  from JDBC so the order in which this method is called
     *  is important.
     */
    private void executeAndStore() throws SQLException {
        String inputText = inputTextArea.getText();
        myDatabase.SqlQueryLookUpStoreLookup(inputText);
    }


    public void connectDB(String Url) throws SQLException {
        myDatabase = new JDBC(Url);
    }

    public static void main(String[] args) throws SQLException {
        GUI example = new GUI();
        example.setVisible(true);
    }
}
