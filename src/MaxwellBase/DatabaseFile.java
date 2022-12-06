package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class DatabaseFile extends RandomAccessFile {
    public final int pageSize;
    public int lastPageIndex = -1;
    public DatabaseFile(String name, Constants.PageType pageType, String path) throws IOException{
        super(path+"/"+name, "rw");
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
        short numberOfCells = getNumberOfCells(page);
        this.seek((long) page * pageSize + 0x02);
        this.writeShort(numberOfCells + 1);
        return (short) (numberOfCells + 1);
    }

    public boolean shouldSplit(int page, int cellSize) throws IOException {
        short numberOfCells = getNumberOfCells(page);
        short headerSize = (short) (0x10 + (2 * numberOfCells + 1));
        return getContentStart(page) - cellSize < headerSize;
    }

    public short getCellOffset(int page, int cellNumber) throws IOException {
        if (cellNumber == -1) {
            return getCellOffset(page, 0);
        }
        this.seek((long) page * pageSize + 0x10 + (2L * cellNumber));
        return this.readShort();
    }

    public int[] getPageInfo(int page) throws IOException {
        int[] pageInfo = new int[5];
        this.seek((long) page * pageSize);
        pageInfo[0] = this.readByte(); // Page type
        this.readByte();
        pageInfo[1] = this.readShort(); // Number of cells
        pageInfo[2] = this.readShort(); // Start of content
        pageInfo[3] = this.readInt(); // Rightmost child page if interior page, Right sibling page if leaf page
        pageInfo[4] = this.readInt(); // Parent page
        return pageInfo;
    }

    public Constants.PageType getPageType(int page) throws IOException {
        this.seek((long) page * pageSize);
        return Constants.PageType.fromValue(this.readByte());
    }

    /**
     * Shifts all cells after precedingCell on page to the front of the page by shift bytes
     * @param page The page to shift cells on
     * @param precedingCell The cell to before the first cell to be shifted
     * @param shift The number of bytes to shift the cells
     * @param newRecord number of records to add or remove
     *                  positive number to add records
     *                  negative number to remove records
     *                  zero to not change the number of records
     * @return The page offset of the free space created
     */
    public int shiftCells(int page, int precedingCell, int shift, int newRecord) throws IOException {
        if (shouldSplit(page, shift)) {
            throw new IOException("Asked to shift cells more than the page can hold");
        }

        int oldContentStart = getContentStart(page);
        int contentOffset = setContentStart(page, (short) shift);
        if (contentOffset == pageSize) {
            return pageSize - shift;
        }
        int startOffset;
        if (precedingCell >= 0) {
            startOffset = getCellOffset(page, precedingCell);
        } else {
            startOffset = pageSize;
        }
        this.seek((long) page * pageSize + oldContentStart);
        int bytesToMove = startOffset - oldContentStart;
        if (shift < 0) {
            bytesToMove += shift;
        }
        byte[] shiftedBytes = new byte[bytesToMove];
        this.read(shiftedBytes);

        this.seek((long) page * pageSize + contentOffset);
        this.write(shiftedBytes);

        int numberOfCells = getNumberOfCells(page);
        // Update the offsets of the cells that were shifted
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1) * 2L);
        byte[] oldOffsets = new byte[(numberOfCells - precedingCell - 1) * 2];
        this.read(oldOffsets);

        // Shift the offsets by shift bytes
        // If we are adding a new record, leave room for it's offset
        // If we are removing a record, remove it's offset
        // If we are not changing the number of records, don't change the offsets
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1 + newRecord) * 2L);

        for (int i = 0; i < oldOffsets.length; i += 2) {
            short oldOffset = (short) ((oldOffsets[i] << 8) | (oldOffsets[i + 1] & 0xFF));
            this.writeShort(oldOffset - shift);
        }
        return startOffset - shift;
    }
}
