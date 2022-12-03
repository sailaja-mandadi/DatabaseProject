package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class DatabaseFile extends RandomAccessFile {
    public final int pageSize;
    public int lastPageIndex = -1;
    public DatabaseFile(String name, Constants.Constants.PageType pageType) throws IOException{
        super(name, "rw");
        this.pageSize = Constants.PAGE_SIZE;
        // If the file is empty, write the first page
        if (this.length() == 0) {
            this.createPage(0xFFFFFFFF, pageType);
        }
    }
    public int createPage(int parentPage, Constants.PageType pageType) throws IOException {
        lastPageIndex++;
        this.setLength((long) (lastPageIndex + 1) * pageSize);
        this.seek((long) lastPageIndex * pageSize);
        this.writeByte(pageType.getValue()); // Page type 0x00
        this.writeByte(0x00); // Unused space 0x01
        this.writeShort(0x00); // Number of cells 0x02
        this.writeShort(pageSize); // Start of content, no content yet so it's the end of the page 0x04
        this.writeInt(0xFFFFFFFF); // Rightmost child page if interior page, Right sibling page if leaf page 0x06
        this.writeInt(parentPage); // Parent page 0x0A
        this.writeShort(0x00); // Unused space 0x0E
        // Fill the rest of the page with 0x00
        while (this.getFilePointer() < pageSize) {
            this.writeByte(0x00);
        }
        return lastPageIndex;
    }

    public short getContentStart(int page) throws IOException {
        this.seek((long) page * pageSize + 0x04);
        return this.readShort();
    }

    public int getParentPage(int page) throws IOException {
        this.seek((long) page * pageSize + 0x0A);
        return this.readInt();
    }

    public int getRootPage() throws IOException {
        int currentPage = 0;
        while (getParentPage(currentPage) != 0xFFFFFFFF) {
            currentPage = getParentPage(currentPage);
        }
        return currentPage;
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

    public boolean shouldSplit(int page, int cellSize) throws IOException {
        short numberOfCells = getNumberOfCells(page);
        short headerSize = (short) (0x10 + (2 * numberOfCells + 1));
        return getContentStart(page) - cellSize < headerSize;
    }

}
