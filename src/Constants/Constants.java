package Constants;
public class Constants {
    public enum PageType {
        INDEX_LEAF(0x0D), INDEX_INTERIOR(0x05), TABLE_LEAF(0x0A), TABLE_INTERIOR(0x0D);
        private final int value;
        private PageType(int value) { this.value = value; }
        public int getValue() { return value; }
    }
    public enum FileType { TABLE, INDEX }
    public enum DataTypes {
        NULL, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, UNUSED, YEAR, TIME, DATETIME, DATE, TEXT
    }

}
