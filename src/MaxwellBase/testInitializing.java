package MaxwellBase;

import Constants.Constants;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class testInitializing {

//    public static boolean isIntialized = false;
    public static String maxwellTables = "maxwellTablesTable";
    public static String maxwellColumns = "maxwellColumnsTable";


    public static void initialize(File dataDirectory) {
        try{
//            Create data directory
            if (dataDirectory.mkdir()){
                initializeDataDirectory();
            }

        }catch (SecurityException e) {
            System.out.println("Unable to create data container directory");
        }
    }
    public static void initializeDataDirectory(){
        try{
            File catalogDirectory = new File(Settings.getCatalogDirectory());;
            File UserDataDirectory = new File(Settings.getUserDataDirectory());;
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
    }
    public static void initializeUserCatalogDirectory(){

        File catalog_directory = new File(Settings.getCatalogDirectory());

        if (catalog_directory.exists()){
            boolean tablesExist = false ;
            boolean columnsExist = false ;
            String[] listOfExistingFiles = catalog_directory.list();
            for (String tableName : listOfExistingFiles)
            {
                if (tableName.equals("maxwellbase_tables.tbl")){
                    tablesExist = true;
                }
                if (tableName.equals("maxwellbase_columns.tbl")){
                    columnsExist  = true;
                }
            }
            if(!tablesExist || !columnsExist){
//            To initialize tables .tbl
                initializeDataDirectory();
            }
        }
        else{
            initializeDataDirectory();
        }

    }

    /**
     * Initializes Maxwell tables table & columns table
     */
    public static void initializeCatalogTables(){

        Table maxwellbase_tables = new Table("maxwellbase_tables");
        Table maxwellbase_columns = new Table("maxwellbase_columns");
        ArrayList<Constants.DataTypes> tableColumns = new ArrayList<>();
        tableColumns.add(Constants.DataTypes.INT);
        tableColumns.add(Constants.DataTypes.TEXT);
        Record rec1 = new Record(tableColumns, new ArrayList<Object>(Arrays.asList(1,"maxwellbase_tables")),1);
        Record rec2 = new Record(tableColumns,new ArrayList<Object>(Arrays.asList(2,"maxwellbase_columns")),2);

        maxwellbase_tables.insert(rec1);
        maxwellbase_tables.insert(rec2);

        ArrayList<Constants.DataTypes> colColumns = new ArrayList<>(Arrays.asList(Constants.DataTypes.INT,
                Constants.DataTypes.TEXT, Constants.DataTypes.TEXT, Constants.DataTypes.TEXT, Constants.DataTypes.TINYINT,
                Constants.DataTypes.TEXT, Constants.DataTypes.TEXT));
        Record rec1 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(1,"maxwellbase_tables","rowid",
                "INT",1,"No","PRI")),1);
        Record rec2 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(2,"maxwellbase_tables","table_name",
                "TEXT",2,"No","UNI")),2);
        Record rec3 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(3,"maxwellbase_columns","rowid",
                "INT",1,"No","PRI")),3);
        Record rec4 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(4,"maxwellbase_columns","table_name",
                "TEXT",2,"No","NULL")),4);
        Record rec5 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(5,"maxwellbase_columns","column_name",
                "TEXT",3,"No","NULL")),5);
        Record rec6 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(6,"maxwellbase_columns","data_type",
                "INT",1,"No","PRI")),6);
        Record rec7 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(7,"maxwellbase_columns","ordinal_position",
                "INT",1,"No","PRI")),7);
        Record rec8 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(8,"maxwellbase_columns","is_nullable",
                "INT",1,"No","PRI")),8);
        Record rec9 = new Record(colColumns, new ArrayList<Object>(Arrays.asList(9,"maxwellbase_columns","column_key",
                "INT",1,"No","PRI")),9);



        maxwellbase_tables.insert(rec1);
        maxwellbase_tables.insert(rec2);





    }

}
