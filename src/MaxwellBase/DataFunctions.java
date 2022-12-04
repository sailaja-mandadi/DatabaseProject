package MaxwellBase;

import Constants.Constants;

public class DataFunctions {

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
            return -1;
        }
        return value1.compareTo(value2);
    }


}
