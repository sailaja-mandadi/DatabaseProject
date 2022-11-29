package MaxwellBase;

import java.io.File;

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

}
