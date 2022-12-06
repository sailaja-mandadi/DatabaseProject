package MaxwellBase;

import Constants.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Initialization {



    public static void initialize(File dataDirectory) throws IOException {
        try{
            if (dataDirectory.mkdir()){
                initializeDataDirectory();
            }
        }catch (SecurityException e) {
            System.out.println("Unable to create data container directory");
        }
    }
    public static void initializeDataDirectory() throws IOException {
        try{
            File catalogDirectory = new File(Settings.getCatalogDirectory());
            File UserDataDirectory = new File(Settings.getUserDataDirectory());
            if (!catalogDirectory.exists()){
                if (!catalogDirectory.mkdir()){
                    System.out.println("Unable to create catalog directory");
                }
            }
            if (!UserDataDirectory.exists()){
                if (!UserDataDirectory.mkdir()){
                    System.out.println("Unable to create user data directory");
                }
            }

            String[] listOfExistingFiles = catalogDirectory.list();
            assert listOfExistingFiles != null;
            for (String tableName : listOfExistingFiles){
                File delFile =  new File(catalogDirectory, tableName);
                if (!delFile.delete()){
                    System.out.println("Unable to delete file");
                }
            }


        }catch (SecurityException e){
            System.out.println("Unable to create data directory");
        }
        // create new maxwellbase_tables and maxwellbase_columns tables
        initializeCatalogTables();
    }
    public static void initializeCatalogDirectory() throws IOException {

        File catalog_directory = new File(Settings.getCatalogDirectory());

        if (catalog_directory.exists()){
            initializeCatalogTables();
        }

    }



    /**
     * Initializes Maxwell tables table & columns table
     */
    public static void initializeCatalogTables() throws IOException {
        File maxwellbase_tables_file = new File(Settings.getCatalogDirectory() + "/" + Settings.maxwellBaseTables + ".tbl");
        boolean tables_exists = maxwellbase_tables_file.exists();
        File maxwellbase_columns_file = new File(Settings.getCatalogDirectory() + "/" + Settings.maxwellBaseColumns + ".tbl");
        boolean columns_exists = maxwellbase_columns_file.exists();
        // create meta data tables
        Table maxwellBaseTables = new Table(
                Settings.maxwellBaseTables, // table name
                new ArrayList<>(List.of("table_name")), // column names
                new ArrayList<>(List.of(Constants.DataTypes.TEXT)), // column types
                new ArrayList<>(List.of(false)), // column nullable
                false // user table
        );
        Table maxwellBaseColumns = new Table(
                Settings.maxwellBaseColumns, // table name
                new ArrayList<>(Arrays.asList(
                        "table_name",
                        "column_name",
                        "data_type",
                        "ordinal_position",
                        "is_nullable",
                        "column_key"
                )), // column names
                new ArrayList<>(Arrays.asList(
                        Constants.DataTypes.TEXT,
                        Constants.DataTypes.TEXT,
                        Constants.DataTypes.TEXT,
                        Constants.DataTypes.TINYINT,
                        Constants.DataTypes.TEXT,
                        Constants.DataTypes.TEXT
                )), // column types
                new ArrayList<>(Arrays.asList(false,false,false,false,false,true)), // column nullable
                false // user table
        );

        Table.tableTable = maxwellBaseTables;
        Table.columnTable = maxwellBaseColumns;

        if (!tables_exists) {
            // insert into tables metadata
            maxwellBaseTables.insert(new ArrayList<>(List.of(Settings.maxwellBaseTables)));
            maxwellBaseTables.insert(new ArrayList<>(List.of(Settings.maxwellBaseColumns)));
        }

        if (!columns_exists) {
            //insert into column metadata
            maxwellBaseColumns.insert(new ArrayList<>(Arrays.asList(Settings.maxwellBaseTables, "table_name",
                    "TEXT", (byte) 1, "No", "PRI")));
            maxwellBaseColumns.insert(new ArrayList<>(Arrays.asList(Settings.maxwellBaseColumns, "table_name",
                    "TEXT", (byte) 1, "No", null)));
            maxwellBaseColumns.insert(new ArrayList<>(Arrays.asList(Settings.maxwellBaseColumns, "column_name",
                    "TEXT", (byte) 2, "No", null)));
            maxwellBaseColumns.insert(new ArrayList<>(Arrays.asList(Settings.maxwellBaseColumns, "data_type",
                    "TEXT", (byte) 3, "No", null)));
            maxwellBaseColumns.insert(new ArrayList<>(Arrays.asList(Settings.maxwellBaseColumns, "ordinal_position",
                 "TINYINT", (byte) 4, "No", null)));
            maxwellBaseColumns.insert(new ArrayList<>(Arrays.asList(Settings.maxwellBaseColumns, "is_nullable",
                    "TEXT", (byte) 5, "No", null)));
            maxwellBaseColumns.insert(new ArrayList<>(Arrays.asList(Settings.maxwellBaseColumns, "column_key",
                    "TEXT", (byte) 6, "No", null)));
        }
    }

}
