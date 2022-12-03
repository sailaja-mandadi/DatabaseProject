package MaxwellBase;

import Constants.Constants;

// FIXME: Rename to DataFunctions it has a typo and jetbrains doesn't let me rename it while not hosting
public class DataFuntions {
    public static boolean compare(Constants.DataTypes columnType, Object value1, String value2, String operator) {
        switch (operator) {
            case "=" -> {
                switch (columnType){
                    case TINYINT, SMALLINT, INT, BIGINT, YEAR, TIME -> {
                        return (Long) value1 == Long.parseLong(value2);
                    }
                    case DATE, DATETIME -> {
                        return Long.compareUnsigned((long) value1, Long.parseUnsignedLong(value2)) == 0;
                    }
                    case FLOAT, DOUBLE -> {
                        return (Double) value1 == Double.parseDouble(value2);
                    }
                    case TEXT -> {
                        return ((String) value1).compareTo(value2) == 0;
                    }
                }
            }
            case "<>" -> {
                switch (columnType){
                    case TINYINT, SMALLINT, INT, BIGINT, YEAR, TIME -> {
                        return (Long) value1 != Long.parseLong(value2);
                    }
                    case DATE, DATETIME -> {
                        return Long.compareUnsigned((long) value1, Long.parseUnsignedLong(value2)) != 0;
                    }
                    case FLOAT, DOUBLE -> {
                        return (Double) value1 != Double.parseDouble(value2);
                    }
                    case TEXT -> {
                        return ((String) value1).compareTo(value2) != 0;
                    }
                }
            }
            case ">" -> {
                switch (columnType){
                    case TINYINT, SMALLINT, INT, BIGINT, YEAR, TIME -> {
                        return (Long) value1 > Long.parseLong(value2);
                    }
                    case DATE, DATETIME -> {
                        return Long.compareUnsigned((long) value1, Long.parseUnsignedLong(value2)) > 0;
                    }
                    case FLOAT, DOUBLE -> {
                        return (Double) value1 > Double.parseDouble(value2);
                    }
                    case TEXT -> {
                        return ((String) value1).compareTo(value2) > 0;
                    }
                }
            }
            case "<" -> {
                switch (columnType){
                    case TINYINT, SMALLINT, INT, BIGINT, YEAR, TIME -> {
                        return (Long) value1 < Long.parseLong(value2);
                    }
                    case DATE, DATETIME -> {
                        return Long.compareUnsigned((long) value1, Long.parseUnsignedLong(value2)) > 0;
                    }
                    case FLOAT, DOUBLE -> {
                        return (Double) value1 < Double.parseDouble(value2);
                    }
                    case TEXT -> {
                        return ((String) value1).compareTo(value2) < 0;
                    }
                }
            }
            case ">=" -> {
                switch (columnType){
                    case TINYINT, SMALLINT, INT, BIGINT, YEAR, TIME -> {
                        return (Long) value1 >= Long.parseLong(value2);
                    }
                    case DATE, DATETIME -> {
                        return Long.compareUnsigned((long) value1, Long.parseUnsignedLong(value2)) >= 0;
                    }
                    case FLOAT, DOUBLE -> {
                        return (Double) value1 >= Double.parseDouble(value2);
                    }
                    case TEXT -> {
                        return ((String) value1).compareTo(value2) >= 0;
                    }
                }
            }
            case "<=" -> {
                switch (columnType){
                    case TINYINT, SMALLINT, INT, BIGINT, YEAR, TIME -> {
                        return (Long) value1 <= Long.parseLong(value2);
                    }
                    case DATE, DATETIME -> {
                        return Long.compareUnsigned((long) value1, Long.parseUnsignedLong(value2)) <= 0;
                    }
                    case FLOAT, DOUBLE -> {
                        return (Double) value1 <= Double.parseDouble(value2);
                    }
                    case TEXT -> {
                        return ((String) value1).compareTo(value2) <= 0;
                    }
                }
            }
            default -> {
                return false;
            }
        }
        return false;
    }
}
