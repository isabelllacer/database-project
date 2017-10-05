package db;

import java.util.ArrayList;
import java.util.List;

public class Column {
    private String name;
    private String kind;
    private List<Entry> entries = new ArrayList<>();

    public Column(String newName, String newKind){
        name = newName;
        kind = newKind;
    }

    public String addEntry(Entry e){
        if (kind.equals(e.gettype())) {
            entries.add(e);
            return "";
        } else {
                return "ERROR: Column and Entry type do not match.";
            }
    }

    public String test_addEntry(Entry e){
        if (kind.equals(e.gettype())) {
            return "";
        } else {
            return "ERROR: Column and Entry type do not match.";
        }
    }




    public int get_numRows(){
        return entries.size();
    }

    public String get_name(){
        return name;
    }

    public String get_kind(){
        return kind;
    }

    public Entry get_entry(int index){
        return entries.get(index);
    }

    public void remove_entry(int index){
        entries.remove(index);
    }


}
