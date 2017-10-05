package db;

import java.util.List;
import java.util.ArrayList;

public class Table {
    private List<Column> columns;

   public Table(){
       columns = new ArrayList<>();
   }

   public void addColumn(Column c){
       columns.add(c);
   }

   public String addRow(List<Entry> ls){
       String tester = "";
       if (ls.size() == columns.size()){
           for (int i = 0; i < ls.size(); i++){
               tester = columns.get(i).test_addEntry(ls.get(i));
               if (tester.contains("ERROR")){
                   return "ERROR: Column and Entry type do not match.";
               }
            }

           for (int i = 0; i < ls.size(); i++){
               columns.get(i).addEntry(ls.get(i));
           }

            return "";
       } else {
           return "ERROR: Number of entries do not match number of columns";
       }
   }

   public int numrows(){
       if (columns.size() != 0){
            return columns.get(0).get_numRows();
       } else {
           return 0;
       }
   }


   public int numcolumns(){
       return columns.size();
    }

    public Column get_column(int index){
       return columns.get(index);
    }

    public ArrayList<Entry> get_row(int index){
        ArrayList<Entry> ls = new ArrayList<>();
        for (Column c : columns){
            ls.add(c.get_entry(index));
        }
        return ls;
    }

    public boolean row_equals(int index, ArrayList<Entry> row2){
        ArrayList<Entry> row1 = get_row(index);
        for (int i = 0; i < row1.size(); i++){
            if (row1.get(i).gettype().equals("float") || row1.get(i).gettype().equals("int")){
                if (row1.get(i).getvalue() != row2.get(i).getvalue()){
                    return false;
                }
            } else {
                if (!row1.get(i).getvalue().equals(row2.get(i).getvalue())){
                    return false;
                }
            }
        }
        return true;
    }

    public void remove_row(int index){
        for (int i = 0; i < numcolumns(); i++){
            columns.get(i).remove_entry(index);
        }
    }


}
