/*
 * SelectStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.db.*;

/**
 * A class that represents a SELECT statement.
 */
public class SelectStatement extends SQLStatement {
    /* Used in the selectList for SELECT * statements. */
    public static final String STAR = "*";
    
    private ArrayList<Object> selectList;
    private Limit limit;
    private boolean distinctSpecified;
    
    /** 
     * Constructs a SelectStatement object involving the specified
     * columns and other objects from the SELECT clause, the specified
     * tables from the FROM clause, the specified conditional
     * expression from the WHERE clause (if any), the specified Limit
     * object summarizing the LIMIT clause (if any), and the specified
     * value indicating whether or not we should eliminate duplicates.
     *
     * @param  selectList  the columns and other objects from the SELECT clause
     * @param  fromList  the list of tables from the FROM clause
     * @param  where  the conditional expression from the WHERE clause (if any)
     * @param  limit  summarizes the info in the LIMIT clause (if any)
     * @param  distinctSpecified  should duplicates be eliminated?
     */
    public SelectStatement(ArrayList<Object> selectList, 
                           ArrayList<Table> fromList, ConditionalExpression where,
                           Limit limit, Boolean distinctSpecified)
    {
        super(fromList, new ArrayList<Column>(), where);
        this.selectList = selectList;
        this.limit = limit;
        this.distinctSpecified = distinctSpecified.booleanValue();
        
        /* add the columns in the select list to the list of columns */
        for (int i = 0; i < selectList.size(); i++) {
            Object selectItem = selectList.get(i);
            if (selectItem instanceof Column)
                this.addColumn((Column)selectItem);
        }

    }
    
    /**
     * Returns a boolean value indicating whether duplicates should be
     * eliminated in the result of this statement -- i.e., whether the
     * user specified SELECT DISTINCT.
     */
    public boolean distinctSpecified() {
        return this.distinctSpecified;
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        try {
            if (!isSupported())
                throw new RuntimeException("Not Supported!");
     
            for (int i = 0; i < this.numTables(); i++) {
                Table table = this.getTable(i);

                // open it first
                if (table.open() != OperationStatus.SUCCESS)
                    throw new RuntimeException("Table not opened or not existed!");
            }

            if (isWhereValid() == false)
                throw new RuntimeException("Where clause is not valid!");


            // create cursor
            RelationIterator it;
            if (this.numTables() == 1)
                it = new TableIterator(this, this.getTable(0), true);
            else
                it = new CrossIterator(this);

            if (this.numColumns() == 0) {
                it.printAll(System.out);
                it.close();
            } else {
                // prepare for projection
                RelationIterator pit = new ProjectionIterator(this, it);
                pit.printAll(System.out);
                pit.close();
            }

        } catch (Exception e) {
            String errMsg = e.getMessage();
            if (errMsg != null)
                System.err.println(errMsg + ".");
            System.err.println("Failed.");
            e.printStackTrace();
        }
    }

    protected boolean isWhereValid() {
        for (int i = 0; i < this.numWhereColumns(); i++) {
            Column c = this.getWhereColumn(i);
            if (this.getColumnByName(c, c.getTable()) == null)
                return false;
        }
        return true;
    }

    protected Column getColumnByName(Column other, Table table) {
        for (int i = 0; i < this.numTables(); i++) {
           
            Table t = this.getTable(i);
           
            for (int j = 0; j < t.numColumns(); j++) {
                Column c = t.getColumn(j);

                if (c.nameMatches(other, table))
                    return c;
            }
        }
        return null;
    }


    protected boolean isSupported() {
        if (this.numTables() == 0)
            return false;
        return true;
    }
}
