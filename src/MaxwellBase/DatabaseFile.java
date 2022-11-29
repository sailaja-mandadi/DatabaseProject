package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class DatabaseFile extends RandomAccessFile {
    public Constants.FileType fileType;
    public int pageSize = 512;
    public int numberOfPages = 0;
    public DatabaseFile(String name, String mode) throws java.io.FileNotFoundException {
        super(name, mode);
    }
    public int createPage(int parentPage) throws IOException {
        numberOfPages++;
        this.setLength(pageSize);
        if (this.fileType == Constants.FileType.TABLE) {
            this.writeByte(Constants.PageType.TABLE_LEAF.getValue());
        } else if (this.fileType == Constants.FileType.INDEX) {
            this.writeByte(Constants.PageType.INDEX_LEAF.getValue());
        }
        this.writeByte(0x00); // Unused
        this.writeShort(0x00); // Number of cells
        this.writeShort(pageSize); // Start of content, no content yet so it's the end of the page
        this.writeInt(0x00); // Rightmost child page if interior page, Right sibling page if leaf page
        this.writeInt(parentPage); // Parent page
        this.writeShort(0x00); // Unused
        // Fill the rest of the page with 0x00
        while (this.getFilePointer() < pageSize) {
            this.writeByte(0x00);
        }
        return numberOfPages;
    }

    public void jumpToPage(int pageNumber) throws IOException {
        this.seek((long) pageNumber * pageSize);
    }

    public short getContentStart(int page) throws IOException {
        this.seek((long) page * pageSize + 0x04);
        return this.readShort();
    }

    public short getParentPage(int page) throws IOException {
        this.seek((long) page * pageSize + 0x0A);
        return this.readShort();
    }

    public short setContentStart(int page, short cellSize) throws IOException {
        short oldContentStart = getContentStart(page);
        short newContentStart = (short) (oldContentStart -  cellSize);
        this.seek((long) page * pageSize + 0x04);
        this.writeShort(newContentStart);
        return newContentStart;
    }

    public short getNumberOfCells(int page) throws IOException {
        this.seek((long) page * pageSize + 0x02);
        return this.readShort();
    }

    public short incrementNumberOfCells(int page) throws IOException {
        this.seek((long) page * pageSize + 0x02);
        short numberOfCells = this.readShort();
        this.seek((long) page * pageSize + 0x02);
        this.writeShort(numberOfCells + 1);
        return (short) (numberOfCells + 1);
    }
}
