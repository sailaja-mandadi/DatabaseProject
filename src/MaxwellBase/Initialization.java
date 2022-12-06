package MaxwellBase;

import Constants.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class Initialization {

//    public static boolean isIntialized = false;
    public static String maxwellTables = "maxwellTablesTable";
    public static String maxwellColumns = "maxwellColumnsTable";


    public static void initialize(File dataDirectory) throws IOException {
        try{
//            Create data directory
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
                catalogDirectory.mkdir();
            }
            if (!UserDataDirectory.exists()){
                UserDataDirectory.mkdir();
            }

            String[] listOfExistingFiles = catalogDirectory.list();
            for (String tableName : listOfExistingFiles){
                File delFile =  new File(catalogDirectory, tableName);
                delFile.delete();
            }


        }catch (SecurityException e){

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
        // create meta data tables
        Table maxwellbase_tables = new Table("maxwellbase_tables",
                new ArrayList<String>(Arrays.asList("table_name")),
                new ArrayList<Constants.DataTypes>(Arrays.asList(Constants.DataTypes.TEXT)),
                new ArrayList<Boolean>(Arrays.asList(false)),false );
        Table maxwellbase_columns = new Table("maxwellbase_columns",
                new ArrayList<String>(Arrays.asList("table_name","column_name","data_type","ordinal_position",
                        "is_nullable","column_key")),
                new ArrayList<Constants.DataTypes>(Arrays.asList(Constants.DataTypes.TEXT,Constants.DataTypes.TEXT,
                Constants.DataTypes.TEXT,Constants.DataTypes.TINYINT,Constants.DataTypes.TEXT,Constants.DataTypes.TEXT))
                ,new ArrayList<Boolean>(Arrays.asList(false,false,false,false,false,true))
                ,false);

        Table.tableTable = maxwellbase_tables;
        Table.columnTable = maxwellbase_columns;

        if (!tables_exists) {
            // insert into tables metadata
            maxwellbase_tables.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_tables")));
            maxwellbase_tables.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_columns")));
        }

        if (!columns_exists) {
            //insert into column metadata
            maxwellbase_columns.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_tables", "table_name",
                    "TEXT", (byte) 1, "No", "PRI")));
            maxwellbase_columns.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_columns", "table_name",
                    "TEXT", (byte) 1, "No", null)));
            maxwellbase_columns.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_columns", "column_name",
                    "TEXT", (byte) 2, "No", null)));
            maxwellbase_columns.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_columns", "data_type",
                    "TEXT", (byte) 3, "No", null)));
            maxwellbase_columns.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_columns", "ordinal_position",
                    "TINYINT", (byte) 4, "No", null)));
            maxwellbase_columns.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_columns", "is_nullable",
                    "TEXT", (byte) 5, "No", null)));
            maxwellbase_columns.insert(new ArrayList<Object>(Arrays.asList("maxwellbase_columns", "column_key",
                    "TEXT", (byte) 6, "No", null)));
        }
    }

}
