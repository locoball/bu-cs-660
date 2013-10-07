/*
 * InsertRow.java
 *
 * DBMS Implementation
 */

import com.sleepycat.db.*;
import com.sleepycat.bind.*;
import com.sleepycat.bind.tuple.*;

/**
 * A class that represents a row that will be inserted in a table in a
 * relational database.
 *
 * This class contains the code used to marshall the values of the
 * individual columns to a single key-data pair in the underlying
 * BDB database.
 */
public class InsertRow {
    private Table table;         // the table in which the row will be inserted
    private Object[] values;     // the individual values to be inserted
    private DatabaseEntry key;   // the key portion of the marshalled row
    private DatabaseEntry data;  // the data portion of the marshalled row
   
    /**
     * Constructs an InsertRow object for a row containing the specified
     * values that is to be inserted in the specified table.
     *
     * @param  t  the table
     * @param  values  the values in the row to be inserted
     */
    public InsertRow(Table table, Object[] values) {
        this.table = table;
        this.values = values;
        
        // These objects will be created by the marshall() method.
        this.key = null;
        this.data = null;
    }
    
    /**
     * Takes the collection of values for this InsertRow
     * and marshalls them into a key/data pair.
     */
    public void marshall() {

        /*
            FORMAT:

            offset headers, 2 bytes * (numOfNonPKColumns + 1)
            colVals

            FORMAT PK:

            if varchar
                LENGTH 2 bytes
                CONTENT
            if non-varchar
                CONTENT
        */
        
        TupleOutput keyTuple = new TupleOutput();
        TupleOutput dataTuple = new TupleOutput();

        // init
        int numOfColumns = table.numColumns();
        boolean hasPK = table.primaryKeyColumn() == null ? false : true;
        int[] offsets = new int[hasPK ? numOfColumns: numOfColumns + 1];

        // write primary key
        if (hasPK) {
            Column pk = table.primaryKeyColumn();
            int type = pk.getType();
            int index = pk.getIndex();
            Object value = values[index];

            if (type == Column.VARCHAR) {
                keyTuple.writeUnsignedShort(((String)value).length());
                keyTuple.writeBytes((String)value);
            } else if (type == Column.INTEGER)
                keyTuple.writeInt(((Integer)value).intValue());
            else if (type == Column.REAL)
                keyTuple.writeDouble(((Double)value).doubleValue());
            else if (type == Column.CHAR)
                keyTuple.writeBytes((String)value);
            else
                throw new RuntimeException("marshall");
        }


        // offset
        int pivot = 0;
        offsets[0] = 2 * offsets.length;

        for (int i = 0; i < numOfColumns; i++) {

            Column currentCol = table.getColumn(i);

            // skip the PK column
            if (currentCol.isPrimaryKey()) 
                continue;

            if (currentCol.getType() == Column.VARCHAR)
                offsets[pivot + 1] = offsets[pivot] + ((String)values[i]).length();
            else
                offsets[pivot + 1] = offsets[pivot] + currentCol.getLength();

            pivot++;
        }

        // write offset as 2-byte arrays
        for (int i = 0; i < offsets.length; i++)
            dataTuple.writeUnsignedShort(offsets[i]);

        // write data
        for (int i = 0; i < numOfColumns; i++) {

            Column currentCol = table.getColumn(i);

            // skip the PK column
            if (currentCol.isPrimaryKey()) 
                continue;

            int type = currentCol.getType();

            if (type == Column.VARCHAR)
                dataTuple.writeBytes((String)values[i]);
            else if (type == Column.INTEGER)
                dataTuple.writeInt(((Integer)values[i]).intValue());
            else if (type == Column.REAL)
                dataTuple.writeDouble(((Double)values[i]).doubleValue());
            else if (type == Column.CHAR)
                dataTuple.writeBytes((String)values[i]);
            else 
                throw new RuntimeException("marshall");
        }

        this.data = new DatabaseEntry(dataTuple.getBufferBytes(), 0, dataTuple.getBufferLength());
        if (hasPK)
            this.key = new DatabaseEntry(keyTuple.getBufferBytes(), 0, keyTuple.getBufferLength());
    }


    /**
     * Returns the DatabaseEntry for the key in the key/data pair for this row.
     *
     * @return  the key DatabaseEntry
     */
    public DatabaseEntry getKey() {
        return this.key;
    }
    
    /**
     * Returns the DatabaseEntry for the data item in the key/data pair 
     * for this row.
     *
     * @return  the data DatabaseEntry
     */
    public DatabaseEntry getData() {
        return this.data;
    }
}
