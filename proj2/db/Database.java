package db;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;


import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;




public class Database {
    private HashMap tables;

    public Database() {
        tables = new HashMap();
    }

    public String transact(String query) {
        return Parse.eval(query, this);

    }

    public String createselect(String newname, String exprs, String[] tablenames, String conds) {
        //execute select statements
        //convert string representation into table
        //add table to database

        //create table t0 as select a from loadBasic1
        if (tables.containsKey(newname)) {
            return "ERROR: Table already exists";
        }

        String sentence = "";
        sentence += "select " + exprs + " from ";
        for (String s : tablenames) {
            sentence += (s + ", ");
        }
        sentence = sentence.substring(0, sentence.length() - 2);

        if (conds != null) {
            sentence += (" where " + conds);
        }

        String tablestring = Parse.eval(sentence, this);

        String[] rows = tablestring.split("\n");
        int numrows = rows.length;

        String[] headers = rows[0].split("\\s*,\\s*");
        int numcols = headers.length;

        Table t = new Table();
        String[] nametype = null;

        for (String s : headers) {
            nametype = s.split("\\s+");
            t.addColumn(new Column(nametype[0], nametype[1]));
        }

        String[] currrow = null;
        //start at second row
        for (int i = 1; i < numrows; i++) {
            currrow = rows[i].split("\\s*,\\s*");
            for (int j = 0; j < numcols; j++) {
                try {
                    int val = Integer.parseInt(currrow[j]);
                    String type = "int";
                    t.get_column(j).addEntry(new Entry(val, type));
                } catch (NumberFormatException e) {
                    try {
                        float val = Float.parseFloat(currrow[j]);
                        String type = "float";
                        t.get_column(j).addEntry(new Entry(val, type));
                    } catch (NumberFormatException ee) {
                        String val = currrow[j];
                        String type = "string";
                        t.get_column(j).addEntry(new Entry(val, type));
                    }
                }
            }
        }

        tables.put(newname, t);


        return "";
    }

    public String applyconds(String tablestring, String conds) {
        if (conds == null || tablestring.contains("ERROR")) {
            return tablestring;
        } else {
            String[] rows = tablestring.split("\n");
            int numrows = rows.length;

            String[] headers = rows[0].split("\\s*,\\s*");
            int numcols = headers.length;

            Table t = new Table();
            String[] nametype = null;

            for (String s : headers) {
                nametype = s.split("\\s+");
                t.addColumn(new Column(nametype[0], nametype[1]));
            }

            String[] currrow = null;
            for (int i = 1; i < numrows; i++) {
                currrow = rows[i].split("\\s*,\\s*");
                for (int j = 0; j < numcols; j++) {
                    try {
                        int val = Integer.parseInt(currrow[j]);
                        String type = "int";
                        t.get_column(j).addEntry(new Entry(val, type));
                    } catch (NumberFormatException e) {
                        try {
                            float val = Float.parseFloat(currrow[j]);
                            String type = "float";
                            t.get_column(j).addEntry(new Entry(val, type));
                        } catch (NumberFormatException ee) {
                            String val = currrow[j];
                            String type = "string";
                            t.get_column(j).addEntry(new Entry(val, type));
                        }
                    }
                }
            }


            try {
                for (String cond : conds.split("\\s*and\\s*")) {
                    t = docond(t, cond);
                }
            } catch (RuntimeException e) {
                return e.getMessage();
            }

            String result = "";
            for (int i = 0; i < t.numcolumns(); i++) {
                result += (t.get_column(i).get_name() + " " + t.get_column(i).get_kind());
                if (i != t.numcolumns() - 1) {
                    result += ",";
                }
            }
            result += "\n";

            ArrayList<Entry> newRow = new ArrayList<>();
            for (int i = 0; i < t.numrows(); i++) {
                newRow = t.get_row(i);
                for (int j = 0; j < newRow.size(); j++) {
                    if (newRow.get(j).gettype().equals("float")) {
                        result += newRow.get(j).floatstring();
                    } else {
                        result += String.valueOf(newRow.get(j).getvalue());
                    }
                    if (j != newRow.size() - 1) {
                        result += ",";
                    }
                }
                if (i != t.numrows() - 1) {
                    result += "\n";
                }
                newRow.clear();
            }
            return result;
        }

    }

    public Table docond(Table t, String cond) {
        String sectype = "literal";
        String op = cond.split("\\s+")[1];
        String col = cond.split("\\s+")[0];
        String sec = cond.split("\\s+")[2];
        Column col2 = null;
        Column col1 = null;

        //what if column name is equal to literal?
        for (int i = 0; i < t.numcolumns(); i++) {
            if (t.get_column(i).get_name().equals(col)) {
                col1 = t.get_column(i);
            }
        }

        if (col1 == null) {
            throw new RuntimeException("ERROR: Cannot find given column in condition");
        }

        for (int i = 0; i < t.numcolumns(); i++) {
            if (t.get_column(i).get_name().equals(sec)) {
                col2 = t.get_column(i);
                sectype = "column";
            }
        }

        ArrayList<Integer> toremove = new ArrayList<>();

        if (sectype.equals("literal")) {
            Object secval = null;
            try {
                secval = Integer.parseInt(sec);
                sectype = "int";
            } catch (NumberFormatException e) {
                try {
                    secval = Float.parseFloat(sec);
                    sectype = "float";
                } catch (NumberFormatException ee) {
                    secval = sec;
                    sectype = "string";
                }
            }

            if (sectype.equals("string") && !col1.get_kind().equals("string")) {
                throw new RuntimeException("ERROR: Cannot compare string to float or int");
            }
            if (!sectype.equals("string") && col1.get_kind().equals("string")) {
                throw new RuntimeException("ERROR: Cannot compare string to float or int");
            }

            String entryval;
            for (int i = 0; i < t.numrows(); i++) {
                if (op.equals("==")) {
                    if (sectype.equals("string")) {
                        if (!col1.get_entry(i).getvalue().equals(secval)) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_entry(i).getvalue() != secval) {
                            toremove.add(i);
                        }
                    }
                } else if (op.equals("!=")) {
                    if (sectype.equals("string")) {
                        if (col1.get_entry(i).getvalue().equals(secval)) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_entry(i).getvalue() == secval) {
                            toremove.add(i);
                        }
                    }
                } else if (op.equals("<")) {
                    if (sectype.equals("string")) {
                        entryval = (String) col1.get_entry(i).getvalue();
                        if (entryval.compareTo((String) secval) >= 0) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && sectype.equals("int")) {
                            if (((int) col1.get_entry(i).getvalue()) >= (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("int") && sectype.equals("float")) {
                            if (((int) col1.get_entry(i).getvalue()) >= (float) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("int")) {
                            if (((float) col1.get_entry(i).getvalue()) >= (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("float")) {
                            if (((float) col1.get_entry(i).getvalue()) >= (float) secval) {
                                toremove.add(i);
                            }
                        }
                    }
                } else if (op.equals(">")) {
                    if (sectype.equals("string")) {
                        entryval = (String) col1.get_entry(i).getvalue();
                        if (entryval.compareTo((String) secval) <= 0) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && sectype.equals("int")) {
                            if (((int) col1.get_entry(i).getvalue()) <= (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("int") && sectype.equals("float")) {
                            if (((int) col1.get_entry(i).getvalue()) <= (float) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("int")) {
                            if (((float) col1.get_entry(i).getvalue()) <= (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("float")) {
                            if (((float) col1.get_entry(i).getvalue()) <= (float) secval) {
                                toremove.add(i);
                            }
                        }
                    }
                } else if (op.equals("<=")) {
                    if (sectype.equals("string")) {
                        entryval = (String) col1.get_entry(i).getvalue();
                        if (entryval.compareTo((String) secval) > 0) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && sectype.equals("int")) {
                            if (((int) col1.get_entry(i).getvalue()) > (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("int") && sectype.equals("float")) {
                            if (((int) col1.get_entry(i).getvalue()) > (float) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("int")) {
                            if (((float) col1.get_entry(i).getvalue()) > (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("float")) {
                            if (((float) col1.get_entry(i).getvalue()) > (float) secval) {
                                toremove.add(i);
                            }
                        }
                    }
                } else if (op.equals(">=")) {
                    if (sectype.equals("string")) {
                        entryval = (String) col1.get_entry(i).getvalue();
                        if (entryval.compareTo((String) secval) < 0) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && sectype.equals("int")) {
                            if (((int) col1.get_entry(i).getvalue()) < (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("int") && sectype.equals("float")) {
                            if (((int) col1.get_entry(i).getvalue()) < (float) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("int")) {
                            if (((float) col1.get_entry(i).getvalue()) < (int) secval) {
                                toremove.add(i);
                            }
                        } else if (col1.get_kind().equals("float") && sectype.equals("float")) {
                            if (((float) col1.get_entry(i).getvalue()) < (float) secval) {
                                toremove.add(i);
                            }
                        }
                    }
                } else {
                    throw new RuntimeException("ERROR: Invalid conditional operator");
                }

            }
        } else { //sec is a column: binary statement
            String kind = col1.get_kind();
            if (col2.get_kind().equals("string") && !col1.get_kind().equals("string")) {
                throw new RuntimeException("ERROR: Cannot compare string to float or int");
            }
            if (!col2.get_kind().equals("string") && col1.get_kind().equals("string")) {
                throw new RuntimeException("ERROR: Cannot compare string to float or int");
            }
            for (int i = 0; i < t.numrows(); i++) {
                if (op.equals("==")) {
                    if (col1.get_kind().equals("string")) {
                        if (!col1.get_entry(i).getvalue().equals(col2.get_entry(i).getvalue())) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_entry(i).getvalue() != col2.get_entry(i).getvalue()) {
                            toremove.add(i);
                        }
                    }
                } else if (op.equals("!=")) {
                    if (col1.get_kind().equals("string")) {
                        if (col1.get_entry(i).getvalue().equals(col2.get_entry(i).getvalue())) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_entry(i).getvalue() == col2.get_entry(i).getvalue()) {
                            toremove.add(i);
                        }
                    }
                } else if (op.equals("<")) {
                    if (sectype.equals("string")) {
                        String entryval = (String) col1.get_entry(i).getvalue();
                        if ((entryval.compareTo((String) col2.get_entry(i).getvalue()) >= 0)) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && col2.get_kind().equals("int")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval >= (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("int") && col2.get_kind().equals("float")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval >= (float) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("int")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if (entryval >= (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("float")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if ((entryval >= (float) col2.get_entry(i).getvalue())) {
                                toremove.add(i);
                            }
                        }
                    }
                } else if (op.equals(">")) {
                    if (col1.get_kind().equals("string")) {
                        String entryval = (String) col1.get_entry(i).getvalue();
                        if (entryval.compareTo((String) col2.get_entry(i).getvalue()) <= 0) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && col2.get_kind().equals("int")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval <= (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("int") && col2.get_kind().equals("float")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval <= (float) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("int")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if (entryval <= (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("float")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if (entryval <= (float) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        }
                    }
                } else if (op.equals("<=")) {
                    if (col1.get_kind().equals("string")) {
                        String entryval = (String) col1.get_entry(i).getvalue();
                        if (entryval.compareTo((String) col2.get_entry(i).getvalue()) > 0) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && col2.get_kind().equals("int")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval > (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("int") && col2.get_kind().equals("float")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval > (float) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("int")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if (entryval > (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("float")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if (entryval > (float) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        }
                    }
                } else if (op.equals(">=")) {
                    if (col1.get_kind().equals("string")) {
                        String entryval = (String) col1.get_entry(i).getvalue();
                        if (entryval.compareTo((String) col2.get_entry(i).getvalue()) < 0) {
                            toremove.add(i);
                        }
                    } else {
                        if (col1.get_kind().equals("int") && col2.get_kind().equals("int")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval < (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("int") && col2.get_kind().equals("float")) {
                            int entryval = (int) col1.get_entry(i).getvalue();
                            if (entryval < (float) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("int")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if (entryval < (int) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        } else if (kind.equals("float") && col2.get_kind().equals("float")) {
                            float entryval = (float) col1.get_entry(i).getvalue();
                            if (entryval < (float) col2.get_entry(i).getvalue()) {
                                toremove.add(i);
                            }
                        }
                    }
                } else {
                    throw new RuntimeException("ERROR: Invalid conditional operator");
                }
            }

        }

        ArrayList<Integer> decremented = new ArrayList<>();

        Set<Integer> hs = new HashSet<>();
        hs.addAll(toremove);
        toremove.clear();
        toremove.addAll(hs);
        Collections.sort(toremove);

        for (int i = 0; i < toremove.size(); i++) {
            t.remove_row(toremove.get(i));
            for (Integer j : toremove) {
                decremented.add(j - 1);
            }
            toremove.clear();
            toremove.addAll(decremented);
            decremented.clear();
        }

        return t;
    }

    public String createfeed(String name, ArrayList<String> colnames, ArrayList<String> colkinds) {
        if (tables.containsKey(name)) {
            return "ERROR: Table already exists.";
        }
        Table t = new Table();
        tables.put(name, t);
        for (int i = 0; i < colnames.size(); i++) {
            t.addColumn(new Column(colnames.get(i), colkinds.get(i)));
        }
        return "";
    }

    //need to check for incorrectly formatted table files
    public String loadtable(String tablename) {
        try {
            FileReader fr = new FileReader(tablename + ".tbl");
            BufferedReader br = new BufferedReader(fr);
            String current = null;

            if ((current = br.readLine()) != null) {
                String[] headers = current.split("\\s*,\\s*");
                ArrayList<String> colnames = new ArrayList<>();
                ArrayList<String> colkinds = new ArrayList<>();
                for (String column : headers) {
                    String[] nametype = column.split("\\s+");
                    try {
                        colnames.add(nametype[0]);
                        colkinds.add(nametype[1]);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        return "ERROR: Malformed column name";
                    }
                }
                if (tables.containsKey(tablename)) {
                    drop(tablename);
                }
                Table t = new Table();
                tables.put(tablename, t);
                for (int i = 0; i < colnames.size(); i++) {
                    t.addColumn(new Column(colnames.get(i), colkinds.get(i)));
                }

                //Load actual entries
                ArrayList<Entry> row = new ArrayList<>();
                String[] splitrow = new String[colnames.size()];
                try {
                    while ((current = br.readLine()) != null) {
                        splitrow = current.split("\\s*,\\s*");

                        for (String s : splitrow) {
                            try {
                                int val = Integer.parseInt(s);
                                String type = "int";
                                row.add(new Entry(val, type));
                            } catch (NumberFormatException e) {
                                try {
                                    //may need to add "f" to s to parse correctly
                                    float val = Float.parseFloat(s);
                                    String type = "float";
                                    row.add(new Entry(val, type));
                                } catch (NumberFormatException ee) {
                                    String val = s;
                                    String type = "string";
                                    row.add(new Entry(val, type));
                                }
                            }
                        }

                        //how to check entries by returning string?
                        String testError = "";
                        testError = insertinto(tablename, row);
                        if (testError.contains("ERROR")) {
                            return testError;
                        }
                        row.clear();
                    }

                } catch (IOException e) {
                    return "ERROR: No next line.";
                }

            } else {
                return "ERROR: Empty table file.";
            }
        } catch (IOException e) {
            return "ERROR: No table file found.";
        }
        return "";
    }

    public String store(String tablename) {
        if (!tables.containsKey(tablename)) {
            return "ERROR: Table does not exist.";
        }
        try {
            PrintWriter pw = new PrintWriter(tablename + ".tbl", "UTF-8");
            pw.write(printtable(tablename));
            pw.close();
        } catch (IOException e) {
            return "ERROR: Cannot create new file";
        }
        return "";
    }

    public String drop(String tablename) {
        if (tables.containsKey(tablename)) {
            tables.remove(tablename);
        } else {
            return "ERROR: This table does not exist.";
        }
        return "";
    }


    public String insertinto(String tablename, List<Entry> row) {
        if (tables.containsKey(tablename)) {
            Table t = (Table) tables.get(tablename);
            return t.addRow(row);
        } else {
            return "ERROR: Table does not exist.";
        }
    }

    public String printtable(String tablename) {
        if (tables.containsKey(tablename)) {
            String result = "";
            Table t = (Table) tables.get(tablename);

            for (int i = 0; i < t.numcolumns(); i++) {
                result += (t.get_column(i).get_name() + " " + t.get_column(i).get_kind());
                if (i != t.numcolumns() - 1) {
                    result += ",";
                }
            }
            result += "\n";

            ArrayList<Entry> newRow = new ArrayList<>();
            for (int i = 0; i < t.numrows(); i++) {
                newRow = t.get_row(i);
                for (int j = 0; j < newRow.size(); j++) {
                    if (newRow.get(j).gettype().equals("float")) {
                        result += newRow.get(j).floatstring();
                    } else {
                        result += String.valueOf(newRow.get(j).getvalue());
                    }
                    if (j != newRow.size() - 1) {
                        result += ",";
                    }
                }
                if (i != t.numrows() - 1) {
                    result += "\n";
                }
                newRow.clear();
            }

            return result;

        } else {
            return "ERROR: Table does not exist.";
        }
    }

    public String selectall(String[] tablenames) {
        for (String name : tablenames) {
            if (!tables.containsKey(name)) {
                return "ERROR: One or more tables do not exist";
            }
        }

        Table t3 = join((Table) tables.get(tablenames[0]), (Table) tables.get(tablenames[1]));

        for (int i = 2; i < tablenames.length; i++) {
            t3 = join(t3, (Table) tables.get(tablenames[i]));
        }


        return tablestring(t3);
    }

    public String columnselect(String colname, String[] tablenames) {
        for (String name : tablenames) {
            if (!tables.containsKey(name)) {
                return "ERROR: One or more tables do not exist";
            }
        }

        Table t3 = null;
        String result = "";

        if (tablenames.length != 1) {
            t3 = join((Table) tables.get(tablenames[0]), (Table) tables.get(tablenames[1]));

            for (int i = 2; i < tablenames.length; i++) {
                t3 = join(t3, (Table) tables.get(tablenames[i]));
            }
        } else {
            t3 = (Table) tables.get(tablenames[0]);
        }

        for (int i = 0; i < t3.numcolumns(); i++) {
            if (t3.get_column(i).get_name().equals(colname)) {
                result = t3.get_column(i).get_name() + " " + t3.get_column(i).get_kind() + "\n";
                for (int j = 0; j < t3.get_column(i).get_numRows(); j++) {
                    if (t3.get_column(i).get_entry(j).gettype().equals("float")) {
                        result += t3.get_column(i).get_entry(j).floatstring();
                    } else {
                        result += String.valueOf(t3.get_column(i).get_entry(j).getvalue());
                    }
                    if (j != t3.get_column(i).get_numRows() - 1) {
                        result += "\n";
                    }
                }

            }
        }

        return result;
    }

    public String mcolselect(String exprs, String[] tablenames) {
        for (String name : tablenames) {
            if (!tables.containsKey(name)) {
                return "ERROR: One or more tables do not exist";
            }
        }

        String[] colnames = exprs.split("\\s*,\\s*");
        String tosplit = colnames[colnames.length - 1];
        String[] splitter = tosplit.split("\\s+");
        //String ncolname = splitter[2];
        colnames[colnames.length - 1] = splitter[0];


        Table t3 = null;
        String result = "";

        if (tablenames.length != 1) {
            t3 = join((Table) tables.get(tablenames[0]), (Table) tables.get(tablenames[1]));

            for (int i = 2; i < tablenames.length; i++) {
                t3 = join(t3, (Table) tables.get(tablenames[i]));
            }
        } else {
            t3 = (Table) tables.get(tablenames[0]);
        }

        //column by column in t3: if column name matches, add header and no new line.
        //row by row in t3: if column name matches, add entry without new line.
        //add new lines instead of commas at the last column (end of each row)


        for (int j = 0; j < colnames.length; j++) {
            for (int i = 0; i < t3.numcolumns(); i++) {
                if (t3.get_column(i).get_name().equals(colnames[j])) {
                    result += t3.get_column(i).get_name() + " " + t3.get_column(i).get_kind();
                    result += ",";
                }
            }
            if (j == colnames.length - 1) {
                result = result.substring(0, result.length() - 1);
                result += "\n";
            }
        }


        for (int i = 0; i < t3.numrows(); i++) {
            for (int j = 0; j < colnames.length; j++) {
                for (int x = 0; x < t3.numcolumns(); x++) {
                    if (t3.get_column(x).get_name().equals(colnames[j])) {
                        if (t3.get_column(x).get_entry(i).gettype().equals("float")) {
                            result += t3.get_column(x).get_entry(i).floatstring();
                        } else {
                            result += String.valueOf(t3.get_column(x).get_entry(i).getvalue());
                        }
                        result += ",";
                    }
                }
                if (j == colnames.length - 1) {
                    result = result.substring(0, result.length() - 1);
                    result += "\n";
                }
            }
        }


        return result;
    }

    public String arithselect(String exprs, String[] tablenames) {
        for (String name : tablenames) {
            if (!tables.containsKey(name)) {
                return "ERROR: One or more tables do not exist";
            }
        }

        Table t3 = null;
        String result = "";

        if (tablenames.length != 1) {
            t3 = join((Table) tables.get(tablenames[0]), (Table) tables.get(tablenames[1]));

            for (int i = 2; i < tablenames.length; i++) {
                t3 = join(t3, (Table) tables.get(tablenames[i]));
            }
        } else {
            t3 = (Table) tables.get(tablenames[0]);
        }

        String[] operands = null;
        String op = null;


        if (exprs.contains("+")) {
            operands = exprs.split("\\s*[+]\\s*");
            op = "+";

        } else if (exprs.contains("-")) {
            operands = exprs.split("\\s*-\\s*");
            op = "-";
        } else if (exprs.contains("*")) {
            operands = exprs.split("\\s*[*]\\s*");
            op = "*";
        } else if (exprs.contains("/")) {
            operands = exprs.split("\\s*/\\s*");
            op = "/";
        } else {
            return "ERROR: Invalid arithmetic operator";
        }

        String ncolname = null;

        try {
            String tosplit = operands[1];
            String[] splitter = tosplit.split("\\s*as\\s*");
            ncolname = splitter[1];
            operands[1] = splitter[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "ERROR: Malformed alias/arithmetic operation";
        }

        Column col1 = null;
        Column col2 = null;
        String secoptype = "literal";

        for (int i = 0; i < t3.numcolumns(); i++) {
            if (t3.get_column(i).get_name().equals(operands[0])) {
                col1 = t3.get_column(i);
            }
        }
        for (int i = 0; i < t3.numcolumns(); i++) {
            if (t3.get_column(i).get_name().equals(operands[1])) {
                col2 = t3.get_column(i);
                secoptype = "column";
            }
        }

        Object secopval = null;

        if (secoptype.equals("column")) {
            if ((col1.get_kind().equals("string") || col2.get_kind().equals("string"))) {
                if (!op.equals("+")) {
                    return "ERROR: Cannot perform operation on String";
                }
            }
            if ((col1.get_kind().equals("string") && !col2.get_kind().equals("string"))) {
                return "ERROR: Cannot perform operations with a string with int or float";
            }

            if (!col1.get_kind().equals("string") && col2.get_kind().equals("string")) {
                return "ERROR: Cannot perform operations with a string with int or float";
            }

        } else {
            try {
                secopval = Integer.parseInt(operands[1]);
                secoptype = "int";
            } catch (NumberFormatException e) {
                try {
                    secopval = Float.parseFloat(operands[1]);
                    secoptype = "float";
                } catch (NumberFormatException ee) {
                    secopval = operands[1];
                    secoptype = "string";
                }
            }
            if (col1.get_kind().equals("string") && !secoptype.equals("string")) {
                return "ERROR: Cannot perform operations with a string with int or float";
            }

            if (!col1.get_kind().equals("string") && secoptype.equals("string")) {
                return "ERROR: Cannot perform operations with a string with int or float";
            }
            if (col1.get_kind().equals("string") && !op.equals("+")) {
                return "ERROR: Cannot perform operation on String";
            }
            if (secoptype.equals("string") && !op.equals("+")) {
                return "ERROR: Cannot perform operation on String";
            }

        }

        String nkind = null;
        if ((col1.get_kind().equals("string"))) {
            nkind = "string";
        } else if ((col1.get_kind().equals("int"))) {
            if (secoptype.equals("column")) {
                if (col2.get_kind().equals("int")) {
                    nkind = "int";
                } else {
                    nkind = "float";
                }
            } else {
                if (secoptype.equals("int")) {
                    nkind = "int";
                } else {
                    nkind = "float";
                }
            }
        } else {
            nkind = "float";

        }

        Column c = new Column(ncolname, nkind);

        //if literal
        if (!secoptype.equals("column")) {
            if (op.equals("+")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (int) secopval, nkind));
                    } else if (nkind.equals("string")) {
                        String val = (String) col1.get_entry(i).getvalue();
                        String nval = (val.substring(0, val.length() - 1));
                        nval += ((String) secopval).substring(1);
                        c.addEntry(new Entry(nval, nkind));
                    } else if (col1.get_kind().equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (float) secopval, nkind));
                    } else if (col1.get_kind().equals("float") && secoptype.equals("int")) {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (int) secopval, nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (float) secopval, nkind));
                    }
                }
            } else if (op.equals("-")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val - (int) secopval, nkind));
                    } else if (col1.get_kind().equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val - (float) secopval, nkind));
                    } else if (col1.get_kind().equals("float") && secoptype.equals("int")) {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val - (int) secopval, nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val - (float) secopval, nkind));
                    }
                }
            } else if (op.equals("*")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (int) secopval, nkind));
                    } else if (col1.get_kind().equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (float) secopval, nkind));
                    } else if (col1.get_kind().equals("float") && secoptype.equals("int")) {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (int) secopval, nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (float) secopval, nkind));
                    }
                }
            } else if (op.equals("/")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (int) secopval, nkind));
                    } else if (col1.get_kind().equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (float) secopval, nkind));
                    } else if (col1.get_kind().equals("float") && secoptype.equals("int")) {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (int) secopval, nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (float) secopval, nkind));
                    }
                }
            }
        } else {
            //if column
            if (op.equals("+")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (int) col2.get_entry(i).getvalue(), nkind));
                    } else if (nkind.equals("string")) {
                        String val = (String) col1.get_entry(i).getvalue();
                        String nval = val.substring(0, val.length() - 1);
                        nval += ((String) col2.get_entry(i).getvalue()).substring(1);
                        c.addEntry(new Entry(nval, nkind));
                    } else if (col1.get_kind().equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (float) col2.get_entry(i).getvalue(), nkind));
                    } else if (col2.get_kind().equals("int")) {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (int) col2.get_entry(i).getvalue(), nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val + (float) col2.get_entry(i).getvalue(), nkind));
                    }
                }
            } else if (op.equals("-")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val - (int) col2.get_entry(i).getvalue(), nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val - (float) col2.get_entry(i).getvalue(), nkind));
                    }
                }
            } else if (op.equals("*")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (int) col2.get_entry(i).getvalue(), nkind));
                    } else if (col1.get_kind().equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (float) col2.get_entry(i).getvalue(), nkind));
                    } else if (col2.get_kind().equals("int")) {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (int) col2.get_entry(i).getvalue(), nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val * (float) col2.get_entry(i).getvalue(), nkind));
                    }
                    float val = (float) col1.get_entry(i).getvalue();
                    new Entry(val * (float) col2.get_entry(i).getvalue(), nkind);
                }
            } else if (op.equals("/")) {
                for (int i = 0; i < t3.numrows(); i++) {
                    if (nkind.equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (int) col2.get_entry(i).getvalue(), nkind));
                    } else if (col1.get_kind().equals("int")) {
                        int val = (int) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (float) col2.get_entry(i).getvalue(), nkind));
                    } else if (col2.get_kind().equals("int")) {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (int) col2.get_entry(i).getvalue(), nkind));
                    } else {
                        float val = (float) col1.get_entry(i).getvalue();
                        c.addEntry(new Entry(val / (float) col2.get_entry(i).getvalue(), nkind));
                    }
                }
            }
        }


        result += c.get_name() + " " + c.get_kind() + "\n";
        for (int i = 0; i < c.get_numRows(); i++) {
            if (c.get_kind().equals("float")) {
                result += c.get_entry(i).floatstring();
            } else {
                result += String.valueOf(c.get_entry(i).getvalue());
            }
            if (i != c.get_numRows() - 1) {
                result += "\n";
            }

        }


        return result;

    }

    public String joincolumns(ArrayList<String> colstringss) {
        int numrows = colstringss.get(0).split("\n").length;
        ArrayList<String[]> splits = new ArrayList<>();
        String result = "";
        for (String col : colstringss) {
            splits.add(col.split("\n"));
        }
        for (int i = 0; i < numrows; i++) {
            for (int j = 0; j < splits.size(); j++) {
                result += splits.get(j)[i];
                if (j != splits.size() - 1) {
                    result += ",";
                }
            }
            if (i != numrows - 1) {
                result += "\n";
            }
        }
        return result;
    }


    public String tablestring(Table t) {
        String result = "";
        for (int i = 0; i < t.numcolumns(); i++) {
            result += (t.get_column(i).get_name() + " " + t.get_column(i).get_kind());
            if (i != t.numcolumns() - 1) {
                result += ",";
            }
        }
        result += "\n";

        ArrayList<Entry> newRow = new ArrayList<>();
        for (int i = 0; i < t.numrows(); i++) {
            newRow = t.get_row(i);
            for (int j = 0; j < newRow.size(); j++) {
                if (newRow.get(j).gettype().equals("float")) {
                    result += newRow.get(j).floatstring();
                } else {
                    result += String.valueOf(newRow.get(j).getvalue());
                }
                if (j != newRow.size() - 1) {
                    result += ",";
                }
            }
            if (i != t.numrows() - 1) {
                result += "\n";
            }
            newRow.clear();
        }
        return result;
    }


    public Table join(Table t1, Table t2) {
        //unnecessary?
        //if (!tables.containsKey(name1) || !tables.containsKey(name2)){
        //    return null;
        //}
        Table t3 = new Table();
        Table shared1 = new Table();
        Table shared2 = new Table();
        Table unshared1 = new Table();
        Table unshared2 = new Table();

        //what if empty tables?


        if (containsmatch(t1, t2)) {


            for (int i = 0; i < t1.numcolumns(); i++) {
                for (int j = 0; j < t2.numcolumns(); j++) {
                    if (t1.get_column(i).get_name().equals(t2.get_column(j).get_name())) {
                        if (t1.get_column(i).get_kind().equals(t2.get_column(j).get_kind())) {
                            String toadd = t1.get_column(i).get_name();
                            t3.addColumn(new Column(toadd, t1.get_column(i).get_kind()));
                            shared1.addColumn(t1.get_column(i));
                            shared2.addColumn(t2.get_column(j));
                        }
                    }
                }
            }

            boolean has = false;
            for (int i = 0; i < t1.numcolumns(); i++) {
                for (int j = 0; j < shared1.numcolumns(); j++) {
                    if (t1.get_column(i).get_name().equals(shared1.get_column(j).get_name())) {
                        has = true;
                    }
                }
                if (!has) {
                    unshared1.addColumn(t1.get_column(i));
                }
                has = false;
            }

            has = false;
            for (int i = 0; i < t2.numcolumns(); i++) {
                for (int j = 0; j < shared2.numcolumns(); j++) {
                    if (t2.get_column(i).get_name().equals(shared2.get_column(j).get_name())) {
                        has = true;
                    }
                }
                if (!has) {
                    unshared2.addColumn(t2.get_column(i));
                }
                has = false;
            }

            for (int i = 0; i < unshared1.numcolumns(); i++) {
                String cname = unshared1.get_column(i).get_name();
                t3.addColumn(new Column(cname, unshared1.get_column(i).get_kind()));
            }
            for (int i = 0; i < unshared2.numcolumns(); i++) {
                String cname = unshared2.get_column(i).get_name();
                t3.addColumn(new Column(cname, unshared2.get_column(i).get_kind()));
            }

            //find which rows in shared1 match rows in shared2
            //for each match in s1 with each match in s2, save indexes of match, add row of shared columns,
            // add row of us1, add row of us2

            //what if nothing is unshared?

            ArrayList<Entry> toAdd = new ArrayList<>();
            for (int i = 0; i < shared1.numrows(); i++) {
                for (int j = 0; j < shared2.numrows(); j++) {
                    if (shared1.row_equals(i, shared2.get_row(j))) {
                        toAdd.addAll(shared1.get_row(i));
                        toAdd.addAll(unshared1.get_row(i));
                        toAdd.addAll(unshared2.get_row(j));
                        t3.addRow(toAdd);
                        toAdd.clear();
                    }
                }
            }


        } else {
            //create columns with names and types
            for (int i = 0; i < t1.numcolumns(); i++) {
                t3.addColumn(new Column(t1.get_column(i).get_name(), t1.get_column(i).get_kind()));
            }
            for (int i = 0; i < t2.numcolumns(); i++) {
                t3.addColumn(new Column(t2.get_column(i).get_name(), t2.get_column(i).get_kind()));
            }

            //Add entries
            ArrayList<Entry> ls = new ArrayList<>();
            for (int i = 0; i < t1.numrows(); i++) {
                for (int j = 0; j < t2.numrows(); j++) {
                    ls.addAll(t1.get_row(i));
                    ls.addAll(t2.get_row(j));
                    t3.addRow(ls);
                    ls.clear();
                }
            }

        }

        return t3;

    }

    public boolean containsmatch(Table t1, Table t2) {
        for (int i = 0; i < t1.numcolumns(); i++) {
            for (int j = 0; j < t2.numcolumns(); j++) {
                if (t1.get_column(i).get_name().equals(t2.get_column(j).get_name())) {
                    if (t1.get_column(i).get_kind().equals(t2.get_column(j).get_kind())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }


}