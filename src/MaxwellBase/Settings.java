package MaxwellBase;

public class Settings {
    static String prompt = "maxwellsql> ";
    static String version = "v1.0";
    static String copyright = "©2022 Team Maxwell";
    static boolean isExit = false;
    static String userDataDirectory = "data/user_data";
    static String catalogDirectory = "data/catalog";

    static String maxwellBaseTables = "maxwellbase_tables";
    static String maxwellBaseColumns = "maxwellbase_columns";

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

    public static String getVersion() {
        return version;
    }

    public static String getCopyright() {
        return copyright;
    }

    /**
     * @param s The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num times.
     */
    public static String line(String s,int num) {
        return String.valueOf(s).repeat(Math.max(0, num));
    }
}