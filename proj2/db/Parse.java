package db;


import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;



public class Parse {
    // Various common constructs, simplifies parsing.
    private static final String REST  = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND   = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD   = Pattern.compile("load " + REST),
            STORE_CMD  = Pattern.compile("store " + REST),
            DROP_CMD   = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD  = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW  = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*" +
            "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS  = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+" +
                    "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+" +
                    "([\\w\\s+\\-*/'<>=!.]+?(?:\\s+and\\s+" +
                    "[\\w\\s+\\-*/'<>=!.]+?)*))?"),
            CREATE_SEL  = Pattern.compile("(\\S+)\\s+as select\\s+" +
                    SELECT_CLS.pattern()),
            INSERT_CLS  = Pattern.compile("(\\S+)\\s+values\\s+(.+?" +
                    "\\s*(?:,\\s*.+?\\s*)*)");

    private static Database db_instance;

    public static String eval(String query, Database db) {
        db_instance = db;
        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
            return createTable(m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            return loadTable(m.group(1));
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            return storeTable(m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            return dropTable(m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            return insertRow(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return printTable(m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(m.group(1));
        } else {
            return "ERROR: Malformed query";
        }
    }

    private static String createTable(String expr) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2).split(COMMA));
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
        } else {
            return "ERROR: Malformed create";
        }
    }

    private static String createNewTable(String name, String[] cols) {
        ArrayList<String> col_kinds = new ArrayList<String>();
        ArrayList<String> col_names = new ArrayList<String>();
        for (int i = 0; i < cols.length; i++) {
            col_kinds.add(cols[i].split("\\s+")[1]); // gets second word in element, the column type
            col_names.add(cols[i].split("\\s+")[0]);
        }

        for (String s: col_kinds){
            if (!s.equals("float") && !s.equals("int") && !s.equals("string")){
                return "ERROR: Invalid column type";
            }
        }

        return db_instance.createfeed(name, col_names, col_kinds);
    }

    private static String createSelectedTable(String name, String exprs, String tables, String conds) {
        String[] table_names = tables.split(COMMA);

        return db_instance.createselect(name, exprs, table_names, conds);
    }

    private static String loadTable(String name) {
        return db_instance.loadtable(name);
    }

    private static String storeTable(String name) {
        return db_instance.store(name);
    }

    private static String dropTable(String name) {
        return db_instance.drop(name);
    }

    private static String insertRow(String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: Malformed insert";
        }

        String[] sep_expr = m.group(2).split(COMMA); //need to do whitespace comma?
        ArrayList<Entry> row_elems = new ArrayList<>();
        for (int i = 0; i < sep_expr.length; i++) {
            try {
                int val = Integer.parseInt(sep_expr[i]);
                String type = "int";
                row_elems.add(new Entry(val, type));
            } catch (NumberFormatException e) {
                try {
                    //may need to add "f" to s to parse correctly
                    float val = Float.parseFloat(sep_expr[i]);
                    String type = "float";
                    row_elems.add(new Entry(val, type));
                } catch (NumberFormatException ee){
                    String val = sep_expr[i];
                    String type = "string";
                    row_elems.add(new Entry(val, type));
                }
            }
        }

        return db_instance.insertinto(m.group(1), row_elems);
    }

    private static String printTable(String name) {
        return db_instance.printtable(name);
    }

    private static String select(String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        Boolean apply = false;
        if (!m.matches()) {
            return "ERROR: Malformed select.";
        }
        if (m.group(3) != null){
            apply = true;
        }
        return select(m.group(1), m.group(2), m.group(3), apply);
    }

    private static String select(String exprs, String tables, String conds, boolean apply) {

        String answer = "";
        ArrayList<String> result = new ArrayList<>();
        if (exprs.contains(",")){
            String[] to_select = exprs.split(COMMA);
            for (int i = 0; i < to_select.length; i++){
                result.add(select(to_select[i], tables, conds, false));
                if (result.get(i).contains("ERROR")){
                    return result.get(i);
                }
            }


            answer = db_instance.joincolumns(result);
            if (apply) {
                return db_instance.applyconds(answer, conds);
            } else {
                return answer;
            }
        }

        if (exprs.contains("+") || exprs.contains("-") || (exprs.contains("*") && !exprs.trim().equals("*")) || exprs.contains("/")){
            answer = select_arith(exprs, tables);
        } else if (exprs.trim().equals("*")){
            answer = select_star(tables);
        } else if (exprs.split(COMMA).length == 1) {
            answer = select_column(exprs, tables);
        }else {
                answer = select_mcol(exprs, tables);
            }

            if (apply) {
            return db_instance.applyconds(answer, conds);
            } else {
            return answer;
            }
    }


    private static String select_arith(String exprs, String tables){
        String[] table_names = tables.split(COMMA);
        return db_instance.arithselect(exprs, table_names);
    }

    private static String select_mcol(String exprs, String tables){
        String[] table_names = tables.split(COMMA);
        return db_instance.mcolselect(exprs, table_names);
    }

    private static String select_column(String exprs, String tables){
        String[] table_names = tables.split(COMMA);
        return db_instance.columnselect(exprs, table_names);

    }

    private static String select_star(String tables){
        String[] table_names = tables.split(COMMA);
        if (table_names.length == 1){
            String[] table_single = new String[2];
            table_single[0] = table_names[0];
            table_single[1] = table_names[0];
            return db_instance.selectall(table_single);
        }

        return db_instance.selectall(table_names);
    }
}

