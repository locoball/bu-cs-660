/*
 * CrossIterator.java
 *
 * DBMS Implementation
 */

import java.io.*;
import com.sleepycat.db.*;
import java.util.*;
/**
 * A class that serves as an iterator over some or all of the tuples
 * in the cross product (i.e., Cartesian product) of two or more
 * tables.
 */
public class CrossIterator extends RelationIterator {
    private TableIterator[] tableIter;
    private Column[] columns;
    private ConditionalExpression where;
    private int numTuples;
    private boolean firstChecked = false;
    
    /**
     * Constructs a CrossIterator object for the subset of the cross
     * product specified by the given SQLStatement.  If the
     * SQLStatement has a WHERE clause, the iterator will only visit
     * rows that satisfy the WHERE clause.
     *
     * @param  stmt  the SQL statement that specifies the cross product
     * @throws IllegalStateException if one of the Table objects in stmt
     *         has not already been opened
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while accessing one of the underlying database(s)
     */
    public CrossIterator(SQLStatement stmt) throws DatabaseException {
        
        // open all the iterators for tables
        // record all columns
        List<TableIterator> its = new ArrayList<TableIterator>();
        List<Column> cols = new ArrayList<Column>();
        int numTables = stmt.numTables();

        for (int i = 0; i < numTables; i++) {
            Table table = stmt.getTable(i);
            TableIterator it = new TableIterator(stmt, table, false);
            its.add(it);

            int numCols = table.numColumns();
            for (int j = 0; j < numCols; j++) {
                Column col = table.getColumn(j);
                col.setTableIterator(it);
                cols.add(col);
            }
        }

        this.tableIter = its.toArray(new TableIterator[its.size()]);
        this.columns = cols.toArray(new Column[cols.size()]);

        // store WHERE
        this.where = stmt.getWhere();
        if (this.where == null)
            this.where = new TrueExpression();

        // init numTuples
        this.numTuples = 0;

        this.initFirst();
    }
    
    /**
     * Closes the iterator, which closes any BDB handles that it is using.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while closing a handle
     */
    public void close() throws DatabaseException {
        for (TableIterator it : this.tableIter) {
            it.close();
        }
    }
    
    /**
     * Advances the iterator to the next tuple in the relation.  If
     * there is a WHERE clause that limits which tuples should be
     * included in the relation, this method will advance the iterator
     * to the next tuple that satisfies the WHERE clause.  If the
     * iterator is newly created, this method will position it on the first
     * tuple in the relation (that satisfies the WHERE clause).
     *
     * @return true if the iterator was advanced to a new tuple, and false
     *         if there are no more tuples to visit
     * @throws DeadlockException if deadlock occurs while accessing the
     *         underlying BDB database(s)
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         while accessing the underlying database(s)
     */
    public boolean next() throws DeadlockException, DatabaseException {

        if (!this.firstChecked) {
            this.firstChecked = true;
            if (this.where.isTrue()) {
                numTuples++;
                return true;
            }
            
        }

        int pivot = this.tableIter.length - 1;

        while (pivot >= 0 && !advance(pivot))
            pivot--;

        if (pivot < 0)
            return false;
        
        if (!this.where.isTrue())
            return next();

        this.numTuples++;
        return true;
    }


    protected void initFirst() throws DeadlockException, DatabaseException {
        for (TableIterator it : this.tableIter) {
            it.first();
        }
    }


    protected boolean advance(int index) throws DeadlockException, DatabaseException {

        TableIterator it = this.tableIter[index];
        if (it.next())
            return true;
        else {
            it.first();
            return false;
        }
    }
    
    /**
     * Gets the column at the specified index in the relation that
     * this iterator iterates over.  The leftmost column has an index of 0.
     *
     * @return  the column
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Column getColumn(int colIndex) {
        return this.columns[colIndex];
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
        Column col = this.columns[colIndex];
        return col.getValue();
    }
    
    public int numColumns() {
        return this.columns.length;
    }
    
    public int numTuples() {
        return this.numTuples;
    }
}
