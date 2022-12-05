package MaxwellBase;

import Constants.Constants;
import java.util.Date;
import java.util.Calendar;

import static java.time.temporal.ChronoUnit.DAYS;

public class DataFunctions {

    /**
     * Takes a string and data type and returns the string parsed to the data type
     * @param dataType The data type to parse the string to
     * @param s The string to parse
     * @return An object of the data type specified
     */
    public static Object parseString(Constants.DataTypes dataType, String s) {
        return switch (dataType) {
            case TINYINT, YEAR -> Byte.parseByte(s);
            case SMALLINT -> Short.parseShort(s);
            case INT, TIME -> Integer.parseInt(s);
            case BIGINT, DATETIME, DATE -> Long.parseLong(s);
            case FLOAT -> Float.parseFloat(s);
            case DOUBLE -> Double.parseDouble(s);
            case TEXT -> s;
            default -> null;
        };
    }

    public static boolean compare(Constants.DataTypes columnType, Object value1, Object value2, String operator) {
        if (value1 == null || value2 == null) {
            return false;
        }
        int comparison = compareTo(columnType, value1, value2);

        return switch (operator) {
            case ">" -> comparison > 0;
            case ">=" -> comparison >= 0;
            case "<" -> comparison < 0;
            case "<=" -> comparison <= 0;
            case "=" -> comparison == 0;
            case "<>" -> comparison != 0;
            default -> false;
        };
    }

    public static boolean compare(Constants.DataTypes columnType, Object value1, String strValue2, String operator) {
        Object value2 = parseString(columnType, strValue2);
        return compare(columnType, value1, value2, operator);
    }

    public static int compareTo(Constants.DataTypes columnType, Object value1, Object value2) {
        return switch (columnType) {
            case TINYINT, YEAR -> compareValues((byte) value1, (byte) value2);
            case SMALLINT -> compareValues((short) value1, (short) value2);
            case INT, TIME -> compareValues((int) value1, (int) value2);
            case BIGINT, DATETIME, DATE -> compareValues((long) value1, (long) value2);
            case FLOAT -> compareValues((float) value1, (float) value2);
            case DOUBLE -> compareValues((double) value1, (double) value2);
            case TEXT -> compareValues((String) value1, (String) value2);
            default -> 0;
        };
    }

    public static <T extends Comparable<T>> int compareValues(T value1, T value2) {
        if (value1 == null || value2 == null) {
            return 1;
        }
        return value1.compareTo(value2);
    }


    /**
     * YEAR to required format of year ( base year - 2000)
     * @param date format - YYYY
     * @return year in String format with 2000 as base year
     */
    public static String toDbYear(String date)
    {
       String k = date.substring(0,4);
       int number = Integer.parseInt(k);
       return String.valueOf(number - 2000);
    }

    /**
     * from  byte to YYYY string
     * @param date format - byte
     * @return string YYYY format
     */
    public static String fromDbYear(byte date)
    {
        int k = (int) date + 2000;
        return String.valueOf(k);
    }


    /**
     * time as milliseconds from mid night
     * @param time - time in hh:mm:ss format
     * @return String time as milliseconds from mid night
     */
    public static String toDbTime(String time) {
        int millis = Integer.parseInt(time.substring(0,2))*3600000 +
                Integer.parseInt(time.substring(3,5))*60000 + Integer.parseInt(time.substring(6,8))*1000;
        return String.valueOf(millis);
    }

    /**
     *
     * @param millis - milliseconds from mid night
     * @return time in hh:mm:ss format
     */
    public static String fromDbTime(int millis) {
        int hrs = millis / 3600000;
        millis = millis%3600000;
        int mins = millis/60000;
        millis = millis%60000;
        int secs = millis/1000;
        return String.format("%02d",hrs) + ":"+String.format("%02d",mins)
                + ":"+String.format("%02d",secs);
    }

    /**
     *
     * @param dateTime - format YYYY-MM-DD_hh:mm:ss
     * @return milli seconds from epoch
     */
    public static String toDbDateTime(String dateTime) {
        Calendar c1 = Calendar.getInstance();
        // set Year
        c1.set(Calendar.YEAR, Integer.parseInt(dateTime.substring(0,4)));
        // set Month
        // MONTH starts with 0 i.e. ( 0 - Jan)
        c1.set(Calendar.MONTH, Integer.parseInt(dateTime.substring(5,7))-1);
        // set Date
        c1.set(Calendar.DATE, Integer.parseInt(dateTime.substring(8,10)));

        c1.set(Calendar.HOUR_OF_DAY, Integer.parseInt(dateTime.substring(11,13)));
        c1.set(Calendar.MINUTE, Integer.parseInt(dateTime.substring(14,16)));
        c1.set(Calendar.SECOND, Integer.parseInt(dateTime.substring(17,19)));
        Date dateOne = c1.getTime();
        //System.out.println("Date: " + dateOne);
        ///System.out.println();
        return String.valueOf(dateOne.getTime());
    }

    /**
     *
     * @param millis - milliseconds from epoch
     * @return date string - YYYY-MM-DD_hh:mm:ss
     */
    public static String fromDbDateTime(long millis ) {
        String date = new java.text.SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new java.util.Date (millis));
        return date;
    }

}
