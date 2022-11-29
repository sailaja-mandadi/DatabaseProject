package MaxwellBase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

public class ExampleFile extends RandomAccessFile {
    public ExampleFile(String name, String mode) throws FileNotFoundException {
        super(name, mode);
        // TODO Auto-generated constructor stub
    }
    static int pageSize;
    static int startOfContent;


    public static final byte TABLE_LEAF_PAGE = 0x0D;
    public static final byte TABLE_INTERIOR_PAGE = 0x05;
    public static final byte NULL     = 0x00;
    public static final byte TINYINT  = 0x04;
    public static final byte SMALLINT = 0x05;
    public static final byte INT      = 0x06;
    public static final byte BIGINT   = 0x07;
    public static final byte REAL     = 0x08;
    public static final byte DOUBLE   = 0x09;
    public static final byte DATETIME = 0x0A;
    public static final byte DATE     = 0x0B;
    public static final byte TEXT     = 0x0C;
    public static final byte VARCHAR  = 0x0C;

    public static void main(String[] args) {
//		File davisBasePath = new File("/Users/cid/Dropbox/UTD/Courses/CS-6360/DavisBase/student.tbl");
        setPageSize(512);
        setStartOfContent(getPageSize());

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0, 0xabcdef33);
        bb.position(0);

        try {
            // Change according to your own file system.
            String filePath = "C:/Users/jmars/IdeaProjects/DatabaseProject";
            RandomAccessFile tableFile = new RandomAccessFile(filePath + "/student.tbl","rw");
            tableFile.setLength(pageSize);
            /* Write page header data */
            tableFile.seek(0);
            tableFile.writeByte(TABLE_LEAF_PAGE);      //  Page type
            tableFile.writeByte(0x00);                 //  Number of records on this page, 0-127 (0x00-0x7f)
            tableFile.writeShort(getStartOfContent()); //  Start of content page offset
            tableFile.writeInt(-1);                    //  Right page pointer, -1 if this is the rightmost leaf



///////////////////////////////////////////////////////////////////////////////
            ByteArrayOutputStream out;
            DataOutputStream dout;
            int rowid;
            String name;
            double gpa;
            short payloadSize;
///////////////////////////////////////////////////////////////////////////////
            /* Create a record payload as a byte array */
            out = new ByteArrayOutputStream();
            dout = new DataOutputStream(out);

            rowid = 1;
            name = "John Smith"; // name
            gpa = 3.5;
            payloadSize = (short) (3 + name.length() + 8);

            dout.writeShort(payloadSize);
            dout.writeInt(rowid);
            dout.writeByte((byte) 2);                   // Number of columns
            dout.writeByte(TEXT + (byte)name.length()); // name TEXT
            dout.writeByte(DOUBLE);                       // gpa REAL
            dout.writeBytes(name);
            dout.writeDouble(3.5);
            System.out.println("dout.size(): " + dout.size());
            System.out.println("out.size(): " + out.size());

            byte[] record = out.toByteArray();
            setStartOfContent((short) (getStartOfContent() - dout.size()));
            tableFile.seek(getStartOfContent());
            tableFile.write(record);

            tableFile.seek(1);
            int x = tableFile.readByte();
            tableFile.seek(8 + (x * 2));
            tableFile.writeShort((short)getStartOfContent());

            incrementRecordCount(tableFile);
            updateStartOfContent(tableFile, (short)getStartOfContent());

///////////////////////////////////////////////////////////////////////////////
            out = new ByteArrayOutputStream();
            dout = new DataOutputStream(out);

            rowid = 2;
            name = "Mary Williams"; // name
            gpa = 3.92;
            payloadSize = (short) (3 + name.length() + 8);

            dout.writeShort(payloadSize);
            dout.writeInt(rowid);
            dout.writeByte((byte) 2);                   // Number of columns
            dout.writeByte(TEXT + (byte)name.length()); // name TEXT
            dout.writeByte(DOUBLE);                       // gpa REAL
            dout.writeBytes(name);
            dout.writeDouble(3.5);
            System.out.println("dout.size(): " + dout.size());
            System.out.println("out.size(): " + out.size());

            record = out.toByteArray();
            setStartOfContent((short) (getStartOfContent() - dout.size()));
            tableFile.seek(getStartOfContent());
            tableFile.write(record);

            tableFile.seek(1);
            x = tableFile.readByte();
            tableFile.seek(8 + (x * 2));
            tableFile.writeShort((short)getStartOfContent());

            incrementRecordCount(tableFile);
            updateStartOfContent(tableFile, (short)getStartOfContent());

///////////////////////////////////////////////////////////////////////////////
            out = new ByteArrayOutputStream();
            dout = new DataOutputStream(out);

            rowid = 3;
            name = "David Wells"; // name
            gpa = 3.19;
            payloadSize = (short) (3 + name.length() + 8);

            dout.writeShort(payloadSize);
            dout.writeInt(rowid);
            dout.writeByte((byte) 2);                   // Number of columns
            dout.writeByte(TEXT + (byte)name.length()); // name TEXT
            dout.writeByte(DOUBLE);                       // gpa REAL
            dout.writeBytes(name);
            dout.writeDouble(3.5);
            System.out.println("dout.size(): " + dout.size());
            System.out.println("out.size(): " + out.size());

            record = out.toByteArray();
            setStartOfContent((short) (getStartOfContent() - dout.size()));
            tableFile.seek(getStartOfContent());
            tableFile.write(record);

            tableFile.seek(1);
            x = tableFile.readByte();
            tableFile.seek(8 + (x * 2));
            tableFile.writeShort((short)getStartOfContent());

            incrementRecordCount(tableFile);
            updateStartOfContent(tableFile, (short)getStartOfContent());

///////////////////////////////////////////////////////////////////////////////
            out = new ByteArrayOutputStream();
            dout = new DataOutputStream(out);

            rowid = 4;
            name = "Barbara Taylor"; // name
            gpa = 3.75;
            payloadSize = (short) (3 + name.length() + 8);

            dout.writeShort(payloadSize);
            dout.writeInt(rowid);
            dout.writeByte((byte) 2);                   // Number of columns
            dout.writeByte(TEXT + (byte)name.length()); // name TEXT
            dout.writeByte(DOUBLE);                       // gpa REAL
            dout.writeBytes(name);
            dout.writeDouble(3.5);
            System.out.println("dout.size(): " + dout.size());
            System.out.println("out.size(): " + out.size());

            record = out.toByteArray();
            setStartOfContent((short) (getStartOfContent() - dout.size()));
            tableFile.seek(getStartOfContent());
            tableFile.write(record);

            tableFile.seek(1);
            x = tableFile.readByte();
            tableFile.seek(8 + (x * 2));
            tableFile.writeShort((short)getStartOfContent());


            incrementRecordCount(tableFile);
            updateStartOfContent(tableFile, (short)getStartOfContent());

///////////////////////////////////////////////////////////////////////////////

            tableFile.close();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    public static int getPageSize() {
        return pageSize;
    }

    public static void setPageSize(int pageSize) {
        ExampleFile.pageSize = pageSize;
    }

    public static int getStartOfContent() {
        return startOfContent;
    }

    public static void setStartOfContent(int startOfContent) {
        ExampleFile.startOfContent = startOfContent;
    }
    public static void incrementRecordCount(RandomAccessFile raf) throws Exception {
        raf.seek(0x01);
        byte count = raf.readByte();
        raf.seek(0x01);
        raf.writeByte(count+1);
    }
    public static void updateStartOfContent(RandomAccessFile raf, short offset) throws Exception {
        raf.seek(0x02);
        raf.writeShort(offset);
    }
}
