/*
 * TableIterator.java
 *
 * DBMS Implementation
 */

import java.io.*;
import com.sleepycat.db.*;
import com.sleepycat.bind.tuple.*;

import javax.xml.bind.DatatypeConverter;

/**
 * A class that serves as an iterator over some or all of the rows in
 * a stored table.  For a given table, there may be more than one
 * TableIterator open at the same time -- for example, when performing the
 * cross product of a table with itself.
 */
public class TableIterator extends RelationIterator {
    private Table table;
    private Cursor cursor;
    private DatabaseEntry key;
    private DatabaseEntry data;
    private ConditionalExpression where;
    private int numTuples;
    
    /**
     * Constructs a TableIterator object for the subset of the specified
     * table that is defined by the given SQLStatement.  If the
     * SQLStatement has a WHERE clause and the evalWhere parameter has a
     * value of true, the iterator will only visit rows that satisfy the
     * WHERE clause.
     *
     * @param  stmt  the SQL statement that defines the subset of the table
     * @param  table the table to iterate over
     * @param  evalWhere should the WHERE clause in stmt be evaluated by this
     *         iterator?  If this iterator is being used by a higher-level
     *         iterator, then we can specify false so that the WHERE clause 
     *         will not be evaluated at this level.
     * @throws IllegalStateException if the specified Table object has not
     *         already been opened
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while accessing one of the underlying database(s)
     */
    public TableIterator(SQLStatement stmt, Table table, boolean evalWhere)
        throws DatabaseException
    {
        this.table = table;
        
        // Make sure the table is open.
        if (table.getDB() == null)
            throw new IllegalStateException("table " + table + " must be " +
              "opened before attempting to create an iterator for it");
        
        /* 
         * Find all columns from the SQL statement whose values will
         * be obtained using this table iterator, and update their
         * state so that we can get their values as needed.
         */
        Column tableCol, stmtCol;
        for (int i = 0; i < table.numColumns(); i++) {
            tableCol = table.getColumn(i);
            // check for a match in the SELECT clause
            for (int j = 0; j < stmt.numColumns(); j++) {
                stmtCol = stmt.getColumn(j);
                if (stmtCol.nameMatches(tableCol, table)) {
                    stmtCol.useColInfo(tableCol);
                    stmtCol.setTableIterator(this);
                }
            }
            // check for a match in the WHERE clause
            for (int j = 0; j < stmt.numWhereColumns(); j++) {
                stmtCol = stmt.getWhereColumn(j);
                if (stmtCol.nameMatches(tableCol, table)) {
                    stmtCol.useColInfo(tableCol);
                    stmtCol.setTableIterator(this);
                }
            }
        }
        
        this.cursor = table.getDB().openCursor(DBMS.getTxn(), null);
        this.key = new DatabaseEntry();
        this.data = new DatabaseEntry();
        
        this.where = (evalWhere ? stmt.getWhere() : null);
        if (this.where == null)
            this.where = new TrueExpression();
        
        this.numTuples = 0;
    }
    
    /**
     * Closes the iterator, which closes any BDB handles that it is using.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while closing a handle
     */
    public void close() throws DatabaseException {
        if (this.cursor != null)
            this.cursor.close();
        this.cursor = null;
    }
    
    /**
     * Positions the iterator on the first tuple in the relation, without
     * taking the a WHERE clause (if any) into effect.
     *
     * Because this method ignores the WHERE clause, it should
     * ordinarily be used only when you need to reposition the cursor
     * at the start of the relation after having completed a previous
     * iteration.
     *
     * @return true if the iterator was advanced to the first tuple, and false
     *         if there are no tuples to visit
     * @throws DeadlockException if deadlock occurs while accessing the
     *         underlying BDB database(s)
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         while accessing the underlying database(s)
     */
    public boolean first() throws DeadlockException, DatabaseException {
        if (this.cursor == null)
            throw new IllegalStateException("this iterator has been closed");
        
        OperationStatus ret = this.cursor.getFirst(this.key, this.data, null); 
        if (ret == OperationStatus.NOTFOUND)
            return false;
        
        /* Only increment num_tuples if the WHERE clause isn't violated. */
        if (this.where.isTrue())
            this.numTuples++;
        
        return true;
    }
    
    /**
     * Advances the iterator to the next tuple in the relation.  If
     * there is a WHERE clause that limits which tuples should be
     * included in the relation, this method will advance the iterator
     * to the next tuple that satisfies the WHERE clause.  If the
     * iterator is newly created, this method will position it on the
     * first tuple in the relation (that satisfies the WHERE clause).
     * Provided that the iterator can be positioned on a tuple, the
     * count of the number of tuples visited by the iterator is
     * incremented.
     *
     * @return true if the iterator was advanced to a new tuple, and false
     *         if there are no more tuples to visit
     * @throws DeadlockException if deadlock occurs while accessing the
     *         underlying BDB database(s)
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         while accessing the underlying database(s)
     */
    public boolean next() throws DeadlockException, DatabaseException {
        
        if (this.cursor == null)
            throw new IllegalStateException("this iterator has been closed");

        OperationStatus ret = this.cursor.getNext(this.key, this.data, null);

        if (ret != OperationStatus.SUCCESS)
            return false;

        if (!this.where.isTrue())
            return next();

        this.numTuples++;
        return true;
    }
    
    /**
     * Gets the column at the specified index in the relation that
     * this iterator iterates over.  The leftmost column has an index of 0.
     *
     * @return  the column
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Column getColumn(int colIndex) {
        return this.table.getColumn(colIndex);
    }
    
    /**
     * Gets the value of the column at the specified index in the row
     * on which this iterator is currently positioned.  The leftmost
     * column has an index of 0.
     *
     * This method will unmarshall the relevant bytes from the
     * key/data pair and return the corresponding Object -- i.e.,
     * an object of type String for CHAR and VARCHAR values, an object
     * of type Integer for INTEGER values, or an object of type Double
     * for REAL values.
     *
     * @return  the value of the column
     * @throws IllegalStateException if the iterator has not yet been
     *         been positioned on a tuple using first() or next()
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Object getColumnVal(int colIndex) {
            
        // SEE InsertRow FOR MARSHALLING PROTOCOL
        boolean hasPK = table.primaryKeyColumn() == null ? false : true;
        Column coldef = table.getColumn(colIndex);
        int type = coldef.getType();
        int length = coldef.getLength();
        Object retval = null;
        TupleInput tuple = null;
        boolean isPK = false;

        if (DBMS.DEBUG) {
            System.out.println("");
            System.out.println("Key: " + DatatypeConverter.printHexBinary(this.key.getData()));
            System.out.println("Data:" + DatatypeConverter.printHexBinary(this.data.getData()));
        }

        //
        if (!hasPK) {
            tuple = new TupleInput(this.data.getData());
            tuple.mark(0);
            
            // jump to the offset of that column
            tuple.skip(colIndex * 2);
            int offset = tuple.readUnsignedShort();
            length = tuple.readUnsignedShort() - offset;

            // jump to the column
            tuple.reset();
            tuple.skip(offset);

        // if it is the key
        } else if (coldef.isPrimaryKey()) {
            tuple = new TupleInput(this.key.getData());
            isPK = true;

        // if it is not the key
        } else {
            int index = colIndex > this.table.primaryKeyColumn().getIndex() ? colIndex - 1 : colIndex;

            tuple = new TupleInput(this.data.getData());
            tuple.mark(0);
            
            // jump to the offset of that column
            tuple.skip(index * 2);
            int offset = tuple.readUnsignedShort();
            length = tuple.readUnsignedShort() - offset;

            // jump to the column
            tuple.reset();
            tuple.skip(offset);
        }

        // read
        retval = doRead(type, length, isPK, tuple);

        return retval;
    }

    protected Object doRead(int type, int length, boolean isPK, TupleInput tuple) {
        Object retval = null;

        if (DBMS.DEBUG) {
            System.out.println();
            System.out.println("Offset:" + tuple.getBufferOffset());
            System.out.println("Length:" + length);
        }

        if (type == Column.VARCHAR) 
            retval = isPK ? tuple.readBytes(tuple.readUnsignedShort()) : tuple.readBytes(length);
        else if (type == Column.INTEGER) 
            retval = new Integer(tuple.readInt());
        else if (type == Column.REAL) 
            retval = new Double(tuple.readDouble());
        else if (type == Column.CHAR)
            retval = tuple.readBytes(length);
        else
            throw new RuntimeException("getColumnVal");

        return retval;
    }
    
    /**
     * Updates the row on which the iterator is positioned, according to
     * the update values specified for the Column objects in the Table object
     * associated with this iterator.
     *
     * @throws IllegalStateException if the iterator has not yet been
     *         been positioned on a row using first() or next()
     * @throws DeadlockException if deadlock occurs while accessing the
     *         underlying BDB database(s)
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         while accessing the underlying database(s)
     */
    public void updateRow() throws DatabaseException, DeadlockException {
        /* not yet implemented */
    }
    
    /**
     * Deletes the row on which the iterator is positioned.
     *
     * @throws IllegalStateException if the iterator has not yet been
     *         been positioned on a row using first() or next()
     * @throws DeadlockException if deadlock occurs while accessing the
     *         underlying BDB database(s)
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         while accessing the underlying database(s)
     */
    public void deleteRow() throws DatabaseException, DeadlockException {
        /* not yet implemented */
    }
    
    public int numTuples() {
        return this.numTuples;
    }
    
    public int numColumns() {
        return this.table.numColumns();
    }
}
