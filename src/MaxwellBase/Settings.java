package MaxwellBase;

public class Settings {
    static String prompt = "maxwellsql> ";
    static String version = "v1.0";
    static String copyright = "©2022 Team Maxwell";
    static boolean isExit = false;
    static String userDataDirectory = "data/user_data";
    static String catalogDirectory = "data/catalog";

    static String maxwellBaseTables = "maxwellBaseTables";
    static String maxwellBaseColumns = "maxwellBaseColumns";
    /*
     * Page size for all files is 512 bytes by default.
     * You may choose to make it user modifiable
     */
    static int pageSize = 512;

    public static String getUserDataDirectory(){
        return userDataDirectory;
    }
    public static String getCatalogDirectory(){
        return catalogDirectory;
    }

    public static boolean isExit() {
        return isExit;
    }

    public static void setExit(boolean e) {
        isExit = e;
    }

    public static String getPrompt() {
        return prompt;
    }

    public static void setPrompt(String s) {
        prompt = s;
    }

    public static String getVersion() {
        return version;
    }

    public static void setVersion(String version) {
        Settings.version = version;
    }

    public static String getCopyright() {
        return copyright;
    }

    public static void setCopyright(String copyright) {
        Settings.copyright = copyright;
    }

    public static int getPageSize() {
        return pageSize;
    }

    public static void setPageSize(int pageSize) {
        Settings.pageSize = pageSize;
    }




    /*
     *  Static method definitions
     */

    /**
     * @param s The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num times.
     */
    public static String line(String s,int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }
}