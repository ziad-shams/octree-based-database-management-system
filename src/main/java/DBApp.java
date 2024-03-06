
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

public class DBApp {

    CSVWriter writer; //writer from OpenCSV library (writes CSV file line by line)
    Vector<String> allTables; //vector (list) of tables in the database
    Vector<String> allIndexes; //vector of indexes in the database
    int maxRows;
    int maxEntries;

    public DBApp() throws DBAppException {
        //initializing the writer
        //the reason we do this here is that each time we make a new writer metadata.csv gets reset so we need to make it one time
        File file = new File("src/main/resources/metadata.csv");
        try {
            FileWriter outputFile = new FileWriter(file);

            writer = new CSVWriter(outputFile);

            //CSV file header
            String[] header = { "Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName","IndexType", "min", "max"
            };
            writer.writeNext(header);

        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }

        //initializing the allTables vector
        allTables = new Vector<String>();
        allIndexes = new Vector<String>();

        //load DBApp.config
        try (InputStream input = new FileInputStream("src/main/resources/DBApp.config")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            maxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
            maxEntries = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));

        } catch (IOException ex) {
            ex.printStackTrace();
            throw new DBAppException(ex.toString());
        }


    }

    public void init(){}

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax )
            throws DBAppException
    {
        //checks if table already exists
        for(String t: allTables)
            if(t==strTableName)
                throw new DBAppException("Table already exists.");

        try {
            //iterates over every column in "htblColNameType"
            Iterator colNameIterator = htblColNameType.keySet().iterator();
            while(colNameIterator.hasNext()) {
                String colName = (String) colNameIterator.next();
                //checks if the current column is the clustering key
                boolean clusteringKey = false;
                clusteringKey = strClusteringKeyColumn.equals(colName);

                if(!htblColNameType.get(colName).equals("java.lang.Integer")
                   && !htblColNameType.get(colName).equals("java.lang.Double")
                   && !htblColNameType.get(colName).equals("java.lang.String")
                   && !htblColNameType.get(colName).equals("java.util.Date"))
                    throw new DBAppException("Column type should be integer, double, string or date");

                if(htblColNameMax.get(colName)==null || htblColNameMin.get(colName)==null)
                    throw new DBAppException("Columns are inconsistent in given arguments");



                //puts one column info in an array
                String[] data = {strTableName,
                                 colName,
                                 htblColNameType.get(colName),
                                 String.valueOf(clusteringKey),
                                 null, null, //null values for index key name & type, will be changed later
                                 htblColNameMin.get(colName),
                                 htblColNameMax.get(colName)};
                //writes it on the csv file
                writer.writeNext(data);
            }
            writer.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }

        //adds the table created to the list of tables
        allTables.add(strTableName);

        //serialize table
        try {
            if(getPage(strTableName,0)!=null)
                return;
            FileOutputStream fileOut =
                    new FileOutputStream(strTableName + "_table");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(new Table(strTableName));
            out.close();
            fileOut.close();
            //System.out.println("Serialized data is saved in " + strTableName + "_table");
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }
    }

    public void insertSorted(Page p, String clusteringKeyColumn, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        //insert sorted
        Object ClusteringKeyValue = htblColNameValue.get(clusteringKeyColumn);
        Object[] pageArray = p.toArray();
        int min = 0;
        int max = pageArray.length - 1;
        int mid = 0;
        Hashtable<String,Object> midTuple = null;
        while( min <= max ){
            mid = (min + max) / 2;
            midTuple = (Hashtable<String, Object>) pageArray[mid];
            if (((Comparable) midTuple.get(clusteringKeyColumn)).compareTo(ClusteringKeyValue) < 0) {
                min = mid + 1;
            } else if (((Comparable) midTuple.get(clusteringKeyColumn)).compareTo(ClusteringKeyValue) > 0) {
                max = mid - 1;
            } else {
                throw new DBAppException("Primary key already exists!");
            }
        }
        if (midTuple != null && mid<p.size())
            if (((Comparable) midTuple.get(clusteringKeyColumn)).compareTo(ClusteringKeyValue) < 0 && mid==p.size()-1)
                p.addElement(htblColNameValue);
            else if (((Comparable) midTuple.get(clusteringKeyColumn)).compareTo(ClusteringKeyValue) > 0)
                p.insertElementAt(htblColNameValue, mid);
            else
                p.insertElementAt(htblColNameValue, mid+1);
        else
            p.addElement(htblColNameValue);
    }

    public Page getPage(String strTableName, int i) throws DBAppException {
        Page p = null;
        try {

            FileInputStream fileIn = new FileInputStream(strTableName + i);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException e) {
            return null;
            //e.printStackTrace();
            //throw new DBAppException(e.toString());
        } catch (ClassNotFoundException c) {
            c.printStackTrace();
            throw new DBAppException(c.toString());
        }

        return p;
    }

    public Page getPage(String filePath) throws DBAppException {
        Page p = null;
        try {

            FileInputStream fileIn = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        } catch (ClassNotFoundException c) {
            c.printStackTrace();
            throw new DBAppException(c.toString());
        }

        return p;
    }

    public void serializePage(Page p, String strTableName, int i) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(strTableName + i);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
            //System.out.println("Serialized data is saved in " + strTableName + i);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }
    }

    public void serializePage(Page p, String filePath) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
            //System.out.println("Serialized data is saved in " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }
    }

    public Octree getOctree(String indexName) throws DBAppException {
        Octree o = null;
        try {

            FileInputStream fileIn = new FileInputStream(indexName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            o = (Octree) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException e) {
            //e.printStackTrace();
            return null;
            //throw new DBAppException(e.toString());
        } catch (ClassNotFoundException c) {
            //c.printStackTrace();
            return null;
            //throw new DBAppException(c.toString());
        }

        return o;
    }

    public void serializeOctree(Octree<String> octree, String indexName) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(indexName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(octree);
            out.close();
            fileOut.close();
            //System.out.println("Serialized data is saved in " + indexName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }
    }

    public void serializeTable(Table t) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(t.getTableName() + "_table");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(t);
            out.close();
            fileOut.close();
            //System.out.println("Serialized data is saved in " + t.getTableName() + "_table");
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }
    }

    public Object parseObject(String s, Class c) throws DBAppException {
        try {
            if (c == Class.forName("java.lang.Integer")) {
                return Integer.parseInt(s);
            }
            if (c == Class.forName("java.util.Date")) {
                DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                return f.parse(s);
            }
            if (c == Class.forName("java.lang.Double")) {
                return Double.parseDouble(s);
            }
            if (c == Class.forName("java.lang.String"))
                return s;
        }
        catch(ClassNotFoundException e){
            throw new DBAppException(e.toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return s;
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException
    {

        //get the table from allTables
        Table t = null;
        for(String x:allTables){
            if(x==strTableName)
                try {

                    FileInputStream fileIn = new FileInputStream(strTableName + "_table");
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    t = (Table) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DBAppException(e.toString());
                } catch (ClassNotFoundException c) {
                    c.printStackTrace();
                    throw new DBAppException(c.toString());
                }
        }
        //if(t==null)
        //    throw new DBAppException("This table does not exist.");

        //iterate over the columns in metadata.csv and compare it to htblColNameValue
        String clusteringKeyColumn = "";
        Vector<String> indexName = new Vector<String>();
        File file = new File("src/main/resources/metadata.csv");
        try {
            FileReader outputFile = new FileReader(file);
            CSVReader reader = new CSVReader(outputFile);
            Vector<String> columnsInTable = new Vector<String>();
            while(true) {
                String[] data = reader.readNext();
                if (data == null)
                    break;
                if(data[0].equals(strTableName)) {
                    columnsInTable.add(data[1]);
                    try {
                        //validate column type
                        if ( !( htblColNameValue.get(data[1]).getClass() == Class.forName(data[2]) ) )
                            throw new DBAppException("Column type is wrong!");
                        Class classy = Class.forName(data[2]);
                        //validate minimum
                        if (((Comparable) htblColNameValue.get(data[1])).compareTo( parseObject(data[6],classy) ) < 0) {
                            throw new DBAppException("Value at column " + data[1] + " is below minimum");
                        }
                        //validate maximum
                        if (((Comparable) htblColNameValue.get(data[1])).compareTo( parseObject(data[7].toLowerCase(),classy) ) > 0) {
                            throw new DBAppException("Value at column " + data[1] + " exceeds maximum");
                        }
                        //get name of the clustering key column for use later
                        if (data[3].equals("true")) {
                            clusteringKeyColumn = data[1];
                        }
                        if(!data[4].equals("")){
                            indexName.add(data[4]);
                        }
                    }
                    catch (NullPointerException e){
                        if(data[3].equals("true"))
                            throw new DBAppException("Primary key cannot be empty");
                        else{
                            htblColNameValue.put(data[1],new NullObject());
                        }
                    }
                }
            }
            //check if columns exist
            for(String c1: htblColNameValue.keySet()){
                boolean flag = false;
                for(String c2: columnsInTable){
                    if(c1.equals(c2))
                        flag = true;
                }
                if(!flag)
                    throw new DBAppException("Column " + c1 + " does not exist");
            }
        } catch (IOException | CsvValidationException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }

        //check if there are no pages; create the first page
        if(t.getAllPages().isEmpty()){
            t.createPage(strTableName + "0");
        }

        //iterate over the pages one by one (linearly)
        Page p = null;
        for(int i=0;i<t.getAllPages().size();i++) {

            //read the file and put it in variable "p"
            while(!t.getAllPages().contains(strTableName+String.valueOf(i))) {
                i++;
                if(t.lastPage.equals(strTableName+(i-1)))
                    t.createPage(strTableName+i);
            }

            p = getPage(strTableName, i);

            //check if it is the page we need to insert the value in, OR the last page
            if(i==t.getAllPages().size()-1
               || (((Comparable)p.firstElement().get(clusteringKeyColumn)).compareTo(htblColNameValue.get(clusteringKeyColumn)) <= 0
                   && ((Comparable)p.lastElement().get(clusteringKeyColumn)).compareTo(htblColNameValue.get(clusteringKeyColumn)) >= 0)
               || (i==0 && ((Comparable)p.firstElement().get(clusteringKeyColumn)).compareTo(htblColNameValue.get(clusteringKeyColumn)) >= 0)){

                //insert sorted
                insertSorted(p,clusteringKeyColumn,htblColNameValue);
                //read variable "p" and write it in the file again
                serializePage(p,strTableName, i);
                serializeTable(t);

                //remove duplicates
                for(int j = 0; j< indexName.size(); j++){
                    String index = indexName.get(j);
                    for(int k = j + 1; k< indexName.size();k++){
                        if(indexName.get(k).equals(index)){
                            indexName.removeElementAt(k);
                            k = j;
                        }
                    }
                }

                //update indexes
                for(int j = 0; j<indexName.size(); j++) {
                    Octree octree = getOctree(indexName.get(j));
                    octree.insert((Comparable) htblColNameValue.get(octree.colX), (Comparable) htblColNameValue.get(octree.colY), (Comparable) htblColNameValue.get(octree.colZ),strTableName + i);
                    serializeOctree(octree,indexName.get(j));
                }

                //debug print
//                System.out.println(i+p.toString());

                //check if the page is full, then
                //move last row in that page to following page and check if following page is also full etc..
                Page curPage = p;
                int j = i;
                while(curPage.size()>maxRows){

                    //if there are no more pages, create one
                    if(j==t.getAllPages().size()-1)
                        t.createPage(strTableName + (j + 1));

                    //removes the last tuple from the current page
                    Hashtable<String,Object> tuple = curPage.lastElement();
                    curPage.remove(curPage.lastElement());
                    serializePage(curPage,strTableName, j);
                    serializeTable(t);

                    //update indexes
                    for(int k = 0; k<indexName.size(); k++) {
                        Octree octree = getOctree(indexName.get(k));
                        octree.remove((Comparable) tuple.get(octree.colX), (Comparable) tuple.get(octree.colY), (Comparable) tuple.get(octree.colZ));
                        serializeOctree(octree,indexName.get(k));
                    }

                    //debug print
//                    System.out.println(j+curPage.toString());

                    //go to the next page
                    j++;
                    while(!t.getAllPages().contains(strTableName+String.valueOf(j))) {
                        j++;
                        if(t.lastPage.equals(strTableName+(j-1)))
                            t.createPage(strTableName + (j));
                    }
                    curPage = getPage(strTableName, j);

                    //insert the tuple into the next page
                    insertSorted(curPage,clusteringKeyColumn,tuple);
                    serializePage(curPage,strTableName, j);

                    //update indexes
                    for(int k = 0; k<indexName.size(); k++) {
                        Octree octree = getOctree(indexName.get(k));
                        octree.insert((Comparable) tuple.get(octree.colX), (Comparable) tuple.get(octree.colY), (Comparable) tuple.get(octree.colZ),strTableName + j);
                        serializeOctree(octree,indexName.get(k));
                    }

                    //debug print
//                    System.out.println(j+curPage.toString());

                }

                //remove from memory
                p = null;
                System.gc();
                break;
            }
        }
        //remove from memory
        t = null;
        System.gc();
    }

    public Hashtable<String, Object> binarySearch(Page p, String strClusteringKeyValue, String clusteringKeyColumn) throws DBAppException {
        Class classy = p.get(0).get(clusteringKeyColumn).getClass();
        Object clusteringKeyValue = parseObject(strClusteringKeyValue,classy);

        Object[] pageArray = p.toArray();
        int min = 0;
        int max = pageArray.length - 1;
        int mid;
        while( min <= max ){
            mid = (min + max) / 2;
            Hashtable<String,Object> midTuple = (Hashtable<String, Object>) pageArray[mid];
            if (((Comparable)midTuple.get(clusteringKeyColumn)).compareTo(clusteringKeyValue) < 0) {
                min = mid + 1;
            } else if (((Comparable)midTuple.get(clusteringKeyColumn)).compareTo(clusteringKeyValue) > 0) {
                max = mid - 1;
            } else {
                return midTuple;
            }
        }
        return null;
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue )
            throws DBAppException{

        //get the table from allTables
        Table t = null;
        for(String x:allTables){
            if(x==strTableName)
                try {

                    FileInputStream fileIn = new FileInputStream(strTableName + "_table");
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    t = (Table) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DBAppException(e.toString());
                } catch (ClassNotFoundException c) {
                    c.printStackTrace();
                    throw new DBAppException(c.toString());
                }
        }
        //if(t==null)
        //    throw new DBAppException("This table does not exist.");

        if(t.getAllPages().isEmpty())
            throw new DBAppException("Record does not exist.");

        //iterate over the columns in metadata.csv and compare it to htblColNameValue
        String clusteringKeyColumn = "";
        String clusteringKeyColumnType = "";
        String clusteringKeyColumnIndexName = "";
        Vector<String> indexName = new Vector<String>();
        File file = new File("src/main/resources/metadata.csv");
        try {
            FileReader outputFile = new FileReader(file);
            CSVReader reader = new CSVReader(outputFile);
            Vector<String> columnsInTable = new Vector<String>();
            while(true) {
                String[] data = reader.readNext();
                if (data == null)
                    break;
                if(data[0].equals(strTableName)) {
                    columnsInTable.add(data[1]);
                    try {
                        //validate column type
                        if (htblColNameValue.get(data[1])!= null && !(htblColNameValue.get(data[1]).getClass() == Class.forName(data[2])))
                            throw new DBAppException("Column type is wrong!");
                        Class classy = Class.forName(data[2]);
                        //validate minimum
                        if (htblColNameValue.get(data[1])!= null && ((Comparable) htblColNameValue.get(data[1])).compareTo( parseObject(data[6],classy) ) < 0) {
                            throw new DBAppException("Value at column " + data[1] + " is below minimum");
                        }
                        //validate maximum
                        if (htblColNameValue.get(data[1])!= null && ((Comparable) htblColNameValue.get(data[1])).compareTo( parseObject(data[7],classy) ) > 0) {
                            throw new DBAppException("Value at column " + data[1] + " exceeds maximum");
                        }
                        //get name of the clustering key column for use later
                        if (data[3].equals("true")) {
                            clusteringKeyColumn = data[1];
                            clusteringKeyColumnType = data[2];
                            if(!data[4].equals("")){
                                clusteringKeyColumnIndexName = data[4];
                            }
                        }
                        if(!data[4].equals("")){
                            indexName.add(data[4]);
                        }
                    }
                    catch (NullPointerException e){
                        if(data[3].equals("true"))
                            throw new DBAppException("Primary key cannot be empty");
                        else{
                            htblColNameValue.put(data[1],new NullObject());
                        }
                    }
                }
            }
            //check if columns exist
            for(String c1: htblColNameValue.keySet()){
                boolean flag = false;
                for(String c2: columnsInTable){
                    if(c1.equals(c2))
                        flag = true;
                }
                if(!flag)
                    throw new DBAppException("Column " + c1 + " does not exist");
            }
        } catch (IOException | CsvValidationException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }

        //remove duplicates
        for(int j = 0; j< indexName.size(); j++){
            String index = indexName.get(j);
            for(int k = j + 1; k< indexName.size();k++){
                if(indexName.get(k).equals(index)){
                    indexName.removeElementAt(k);
                    k = j;
                }
            }
        }

        //update by index?
        if(clusteringKeyColumnIndexName.equals("")){
            //do nothing
        } else {
            System.out.println("Update by index was used!");
            Octree index = getOctree(clusteringKeyColumnIndexName);
            SQLTerm term1 = new SQLTerm();
            term1._strOperator = "=";
            term1._strTableName = strTableName;
            term1._strColumnName = index.colX;
            SQLTerm term2 = new SQLTerm();
            term2._strOperator = "=";
            term2._strTableName = strTableName;
            term2._strColumnName = index.colY;
            SQLTerm term3 = new SQLTerm();
            term3._strOperator = "=";
            term3._strTableName = strTableName;
            term3._strColumnName = index.colZ;
            try {
                if(index.colX.equals(clusteringKeyColumn)) {
                    term1._objValue = parseObject(strClusteringKeyValue, Class.forName(clusteringKeyColumnType));
                    term2._objValue = new NullObject();
                    term3._objValue = new NullObject();
                }
                else if(index.colX.equals(clusteringKeyColumn)) {
                    term2._objValue = parseObject(strClusteringKeyValue, Class.forName(clusteringKeyColumnType));
                    term1._objValue = new NullObject();
                    term3._objValue = new NullObject();
                }
                else {
                    term3._objValue = parseObject(strClusteringKeyValue, Class.forName(clusteringKeyColumnType));
                    term2._objValue = new NullObject();
                    term1._objValue = new NullObject();
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            Vector<String> pagesToUpdate = index.getIf(new SQLTerm[]{term1, term2, term3});

            //remove duplicates
            for(int j = 0; j< pagesToUpdate.size(); j++){
                String page = pagesToUpdate.get(j);
                for(int k = j + 1; k< pagesToUpdate.size();k++){
                    if(pagesToUpdate.get(k).equals(page)){
                        pagesToUpdate.removeElementAt(k);
                        k = j;
                    }
                }
            }

            loop: for(String pageFilePath: pagesToUpdate) {
                Page p = getPage(pageFilePath);
                Hashtable<String,Object> tuple = binarySearch(p,strClusteringKeyValue,clusteringKeyColumn);
                if(tuple==null){
                    continue loop;
                }
                if(htblColNameValue.get(clusteringKeyColumn) != null){
                    throw new DBAppException("htblColNameValue cannot contain primary key");
                }
                //update indexes
                for(int j = 0; j<indexName.size(); j++) {
                    Octree octree = getOctree(indexName.get(j));
                    octree.remove((Comparable) tuple.get(octree.colX), (Comparable) tuple.get(octree.colY), (Comparable) tuple.get(octree.colZ));
                    serializeOctree(octree,indexName.get(j));
                }
                tuple.putAll(htblColNameValue);
                serializePage(p, pageFilePath);
                serializeTable(t);
                System.out.println(p.toString());
                //update indexes
                for(int j = 0; j<indexName.size(); j++) {
                    Octree octree = getOctree(indexName.get(j));
                    octree.insert((Comparable) tuple.get(octree.colX), (Comparable) tuple.get(octree.colY), (Comparable) tuple.get(octree.colZ), pageFilePath);
                    serializeOctree(octree,indexName.get(j));
                }
                //remove from memory
                p = null;
                t = null;
                System.gc();
                return;
            }
        }

        //update by linear search!
        //iterate over the pages one by one (linearly)
        Page p = null;
        for(int i=0;i<t.getAllPages().size();i++) {

            //read the file and put it in variable "p"
            while(!t.getAllPages().contains(strTableName+String.valueOf(i))) {
                i++;
                if(t.lastPage.equals(strTableName+(i-1)))
                    break;
            }

            p = getPage(strTableName, i);

            Class classy = null;
            try {
                classy = Class.forName(clusteringKeyColumnType);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            //check if it is the page we need to insert the value in, OR the last page
            if (i==t.getAllPages().size()-1
                   || (((Comparable)p.firstElement().get(clusteringKeyColumn)).compareTo(parseObject(strClusteringKeyValue, classy)) <= 0
                   && ((Comparable)p.lastElement().get(clusteringKeyColumn)).compareTo(parseObject(strClusteringKeyValue, classy)) >= 0)) {

                //binary search!!
                Hashtable<String,Object> tuple = binarySearch(p,strClusteringKeyValue,clusteringKeyColumn);
                if(tuple==null){
                    throw new DBAppException("Tuple not found!");
                }

                if(htblColNameValue.get(clusteringKeyColumn) != null &&
                   !tuple.get(clusteringKeyColumn).toString().equals(htblColNameValue.get(clusteringKeyColumn).toString())){
                    throw new DBAppException("Cannot change the primary key!");
                }

                if(htblColNameValue.get(clusteringKeyColumn) != null){
                    throw new DBAppException("htblColNameValue cannot contain primary key");
                }

                //update indexes
                for(int j = 0; j<indexName.size(); j++) {
                    Octree octree = getOctree(indexName.get(j));
                    octree.remove((Comparable) tuple.get(octree.colX), (Comparable) tuple.get(octree.colY), (Comparable) tuple.get(octree.colZ));
                    serializeOctree(octree,indexName.get(j));
                }

                tuple.putAll(htblColNameValue);
                serializePage(p, strTableName, i);
                serializeTable(t);

                //update indexes
                for(int j = 0; j<indexName.size(); j++) {
                    Octree octree = getOctree(indexName.get(j));
                    octree.insert((Comparable) tuple.get(octree.colX), (Comparable) tuple.get(octree.colY), (Comparable) tuple.get(octree.colZ), strTableName + i);
                    serializeOctree(octree,indexName.get(j));
                }

                //debug print
//                System.out.println(i+p.toString());

                //remove from memory
                p = null;
                t = null;
                System.gc();
                break;
            }
        }
    }


    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue )
            throws DBAppException{

        //get the table from allTables
        Table t = null;
        for(String x:allTables){
            if(x==strTableName)
                try {

                    FileInputStream fileIn = new FileInputStream(strTableName + "_table");
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    t = (Table) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DBAppException(e.toString());
                } catch (ClassNotFoundException c) {
                    c.printStackTrace();
                    throw new DBAppException(c.toString());
                }
        }
        //if(t==null)
        //    throw new DBAppException("This table does not exist.");

        //iterate over the columns in metadata.csv and compare it to htblColNameValue
        String clusteringKeyColumn = "";
        Vector<String> indexName = new Vector<String>();
        Hashtable<String, String> indexOfEach = new Hashtable<>();
        File file = new File("src/main/resources/metadata.csv");
        try {
            FileReader outputFile = new FileReader(file);
            CSVReader reader = new CSVReader(outputFile);
            Vector<String> columnsInTable = new Vector<String>();
            while(true) {
                String[] data = reader.readNext();
                if (data == null)
                    break;
                if(data[0].equals(strTableName)) {
                    columnsInTable.add(data[1]);
                    //validate column type
                    if (htblColNameValue.get(data[1])!= null && !(htblColNameValue.get(data[1]).getClass()==Class.forName(data[2])))
                        throw new DBAppException("Column type is wrong!");
                    Class classy = Class.forName(data[2]);
                    //validate minimum
                    if (htblColNameValue.get(data[1])!= null && ((Comparable) htblColNameValue.get(data[1])).compareTo( parseObject(data[6],classy) ) < 0) {
                        throw new DBAppException("Value at column " + data[1] + " is below minimum");
                    }
                    //validate maximum
                    if (htblColNameValue.get(data[1])!= null && ((Comparable) htblColNameValue.get(data[1])).compareTo( parseObject(data[7],classy) ) > 0) {
                        throw new DBAppException("Value at column " + data[1] + " exceeds maximum");
                    }
                    //get name of the clustering key column for use later
                    if (data[3].equals("true")){
                        clusteringKeyColumn = data[1];
                    }
                    if(!data[4].equals("")){
                        indexName.add(data[4]);
                        indexOfEach.put(data[1],data[4]);
                    }
                }
            }
            //check if columns exist
            for(String c1: htblColNameValue.keySet()){
                boolean flag = false;
                for(String c2: columnsInTable){
                    if(c1.equals(c2))
                        flag = true;
                }
                if(!flag)
                    throw new DBAppException("Column " + c1 + " does not exist");
            }
        } catch (IOException | CsvValidationException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException(e.toString());
        }

        //remove duplicates
        for(int j = 0; j< indexName.size(); j++){
            String index = indexName.get(j);
            for(int k = j + 1; k< indexName.size();k++){
                if(indexName.get(k).equals(index)){
                    indexName.removeElementAt(k);
                    k = j;
                }
            }
        }

        //delete by index?
        boolean updateByIndex = false;
        String indexToBeUsed = "";
        for(int i = 0; i<htblColNameValue.size(); i++){
            if(indexOfEach.get(htblColNameValue.keySet().toArray()[i])!=null) {
                updateByIndex = true;
                indexToBeUsed = indexOfEach.get(htblColNameValue.keySet().toArray()[i]);
            }
        }
        if (updateByIndex) {
            System.out.println("Delete by index was used!");
            Octree index = getOctree(indexToBeUsed);
            SQLTerm term1 = new SQLTerm();
            term1._strOperator = "=";
            term1._strTableName = strTableName;
            term1._strColumnName = index.colX;
            SQLTerm term2 = new SQLTerm();
            term2._strOperator = "=";
            term2._strTableName = strTableName;
            term2._strColumnName = index.colY;
            SQLTerm term3 = new SQLTerm();
            term3._strOperator = "=";
            term3._strTableName = strTableName;
            term3._strColumnName = index.colZ;

            if(htblColNameValue.containsKey(index.colX))
                term1._objValue = htblColNameValue.get(index.colX);
            else
                term1._objValue = new NullObject();
            if(htblColNameValue.containsKey(index.colY))
                term2._objValue = htblColNameValue.get(index.colY);
            else
                term2._objValue = new NullObject();
            if (htblColNameValue.containsKey(index.colZ))
                term3._objValue = htblColNameValue.get(index.colZ);
            else
                term3._objValue = new NullObject();

            Vector<String> pagesToUpdate = index.getIf(new SQLTerm[]{term1, term2, term3});

            //remove duplicates
            for(int j = 0; j< pagesToUpdate.size(); j++){
                String page = pagesToUpdate.get(j);
                for(int k = j + 1; k< pagesToUpdate.size();k++){
                    if(pagesToUpdate.get(k).equals(page)){
                        pagesToUpdate.removeElementAt(k);
                        k = j;
                    }
                }
            }

            loop: for(String pageFilePath: pagesToUpdate) {
                Page p = getPage(pageFilePath);
                Vector<Hashtable<String,Object>> tuplesToBeDeleted = new Vector<Hashtable<String,Object>>();
                for(String c: htblColNameValue.keySet()){
                    if(c.equals(clusteringKeyColumn)){
                        Hashtable<String,Object> tuple = binarySearch(p,htblColNameValue.get(clusteringKeyColumn).toString(), clusteringKeyColumn);
                        p.remove(tuple);
                        serializePage(p, pageFilePath);
                        serializeTable(t);
                        //System.out.println("Binary search was performed to delete tuple..");
                        //System.out.println(p.toString());
                        for(int j = 0; j<indexName.size() && tuple!=null; j++) {
                            Octree octree = getOctree(indexName.get(j));
                            octree.remove((Comparable) tuple.get(octree.colX), (Comparable) tuple.get(octree.colY), (Comparable) tuple.get(octree.colZ));
                            serializeOctree(octree,indexName.get(j));
                        }
                        return;
                    }
                }
                for(Hashtable<String,Object> tuple: p){
                    Set columnSet = htblColNameValue.keySet();
                    Boolean flag = true;
                    for(Object column: columnSet){
                        if(!tuple.get((String) column).equals(htblColNameValue.get((String) column)))
                            flag = false;
                    }
                    if(flag)
                        tuplesToBeDeleted.add(tuple);
                }
                while(!tuplesToBeDeleted.isEmpty()){
                    //update indexes
                    for(int j = 0; j<indexName.size(); j++) {
                        Octree octree = getOctree(indexName.get(j));
                        octree.remove((Comparable) tuplesToBeDeleted.get(0).get(octree.colX), (Comparable) tuplesToBeDeleted.get(0).get(octree.colY), (Comparable) tuplesToBeDeleted.get(0).get(octree.colZ));
                        serializeOctree(octree,indexName.get(j));
                    }
                    p.remove(tuplesToBeDeleted.remove(0));
                }
                serializePage(p, pageFilePath);
                serializeTable(t);

                if(p.isEmpty()){
                    t.deletePage(p);
                    serializeTable(t);
                }

                //debug print
//                System.out.println(p.toString());

                //remove from memory
                p = null;
                System.gc();
            }
            t = null;
            System.gc();
            return;
        }

        //iterate over the pages one by one (linearly)
        Page p = null;
        for(int i=0;i<t.getAllPages().size();i++) {


            while(!t.getAllPages().contains(strTableName+String.valueOf(i))) {
                i++;
                if(t.lastPage.equals(strTableName+(i-1)))
                    break;
            }

            //read the file and put it in variable "p"
            p = getPage(strTableName, i);

            //check if it is the page we need to delete the value in, OR the last page
//          if (i == t.getAllPages().size() - 1
//                  || (p.firstElement().get(clusteringKeyColumn).toString().compareTo(htblColNameValue.get(clusteringKeyColumn).toString()) <= 0
//                  && p.lastElement().get(clusteringKeyColumn).toString().compareTo(htblColNameValue.get(clusteringKeyColumn).toString()) >= 0)) {

            Vector<Hashtable<String,Object>> tuplesToBeDeleted = new Vector<Hashtable<String,Object>>();

            for(String c: htblColNameValue.keySet()){
                if(c.equals(clusteringKeyColumn)){
                    Hashtable<String,Object> tuple = binarySearch(p,htblColNameValue.get(clusteringKeyColumn).toString(), clusteringKeyColumn);
                    p.remove(tuple);
                    serializePage(p, strTableName, i);
                    serializeTable(t);
                    //System.out.println("Binary search was performed to delete tuple..");
                    System.out.println(i+p.toString());
                    return;
                }
            }

            for(Hashtable<String,Object> tuple: p){
                Set columnSet = htblColNameValue.keySet();
                Boolean flag = true;
                for(Object column: columnSet){
                    if(!tuple.get((String) column).equals(htblColNameValue.get((String) column)))
                        flag = false;
                }
                if(flag)
                    tuplesToBeDeleted.add(tuple);
            }

            while(!tuplesToBeDeleted.isEmpty()){
                //update indexes
                for(int j = 0; j<indexName.size(); j++) {
                    Octree octree = getOctree(indexName.get(j));
                    octree.remove((Comparable) tuplesToBeDeleted.get(0).get(octree.colX), (Comparable) tuplesToBeDeleted.get(0).get(octree.colY), (Comparable) tuplesToBeDeleted.get(0).get(octree.colZ));
                    serializeOctree(octree,indexName.get(j));
                }
                p.remove(tuplesToBeDeleted.remove(0));
            }
            serializePage(p, strTableName, i);
            serializeTable(t);

            if(p.isEmpty()){
                t.deletePage(p);
                serializeTable(t);
            }

            //debug print
//            System.out.println(i+p.toString());

            //remove from memory
            p = null;
            System.gc();
        }
        //remove from memory
        t = null;
        System.gc();
    }

    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException {

        //validate input
        if (strarrColName.length != 3)
            throw new DBAppException("Three columns are required.");

        //get the table from allTables
        Table t = null;
        for (String x : allTables) {
            if (x == strTableName)
                try {
                    FileInputStream fileIn = new FileInputStream(strTableName + "_table");
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    t = (Table) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DBAppException(e.toString());
                } catch (ClassNotFoundException c) {
                    c.printStackTrace();
                    throw new DBAppException(c.toString());
                }
        }
        //if (t == null)
        //    throw new DBAppException("This table does not exist.");

        //initiate some useful variables
        String clusteringKeyColumn = "";
        Vector<String> columnsInTable = new Vector<String>();
        Vector<String> indexInTable = new Vector<String>();
        Hashtable<String, String> maximumsOfEach = new Hashtable<String, String>();
        Hashtable<String, String> minimumsOfEach = new Hashtable<String, String>();
        Hashtable<String, String> typesOfEach = new Hashtable<String, String>();

        //name of index
        String indexName = strarrColName[0] + strarrColName[1] + strarrColName[2] + "Index";

        //extract columns and indexes used in the table from metadata.csv
        File file = new File("src/main/resources/metadata.csv");
        try {
            FileReader outputFile = new FileReader(file);
            CSVReader reader = new CSVReader(outputFile);

            Vector<String[]> allData = new Vector<String[]>();

            while (true) {
                String[] data = reader.readNext();
                if (data == null)
                    break;
                if (data[0].equals(strTableName)) {
                    columnsInTable.add(data[1]);
                    for (String s : strarrColName) {
                        //if the column is one we want to create an index on, check if it already has one, else add it
                        if (data[1].equals(s)) {
                            if (!(data[4].equals(""))) {
                                throw new DBAppException("One of the columns already has an index");
                            } else {
                                data[4] = indexName;
                                data[5] = "Octree";
                            }
                            maximumsOfEach.put(data[1], data[7]);
                            minimumsOfEach.put(data[1], data[6]);
                            typesOfEach.put(data[1], data[2]);
                        }
                    }
                    indexInTable.add(data[4]);
                    if (data[3].equals("true")) {
                        clusteringKeyColumn = data[1];
                    }
                }
                allData.add(data);
            }

            //delete metadata and rewrite it all again with new index
            FileWriter inputFile = new FileWriter(file);
            CSVWriter writerTwo = new CSVWriter(inputFile);
            writerTwo.writeAll(allData);
            writerTwo.flush();
            writer = writerTwo;

        } catch (IOException | CsvValidationException e) {
            throw new DBAppException(e.toString());
        }

        if(getOctree(indexName)!=null){
            return;
        }

        Object x1 = null, y1 = null, z1 = null, x2 = null, y2 = null, z2 = null;
        try {

            x1 = parseObject(minimumsOfEach.get(strarrColName[0]), Class.forName(typesOfEach.get(strarrColName[0])));
            y1 = parseObject(minimumsOfEach.get(strarrColName[1]), Class.forName(typesOfEach.get(strarrColName[1])));
            z1 = parseObject(minimumsOfEach.get(strarrColName[2]), Class.forName(typesOfEach.get(strarrColName[2])));
            x2 = parseObject(maximumsOfEach.get(strarrColName[0]), Class.forName(typesOfEach.get(strarrColName[0])));
            y2 = parseObject(maximumsOfEach.get(strarrColName[1]), Class.forName(typesOfEach.get(strarrColName[1])));
            z2 = parseObject(maximumsOfEach.get(strarrColName[2]), Class.forName(typesOfEach.get(strarrColName[2])));

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        allIndexes.add(indexName);
        Octree<String> octree = new Octree<String>((Comparable) x1,(Comparable) y1,(Comparable) z1, (Comparable) x2, (Comparable) y2, (Comparable) z2, strarrColName[0], strarrColName[1], strarrColName[2]);

        //iterate over the pages one by one (linearly)
        Page p = null;
        for(int i=0;i<t.getAllPages().size();i++) {

            //read the file and put it in variable "p"
            while (!t.getAllPages().contains(strTableName + String.valueOf(i))) {
                i++;
                if(t.lastPage.equals(strTableName+(i-1)))
                    break;
            }
            p = getPage(strTableName, i);

            //linear search over the page
            for (int j = 0; j < p.size(); j++) {
                Hashtable<String, Object> tuple = p.get(j);
                Object x = tuple.get(strarrColName[0]);
                Object y = tuple.get(strarrColName[1]);
                Object z = tuple.get(strarrColName[2]);
                octree.insert( (Comparable) x, (Comparable) y, (Comparable) z, strTableName + i);
            }
        }
        serializeOctree(octree, indexName);
    }


    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators)
            throws DBAppException {

        //check if input is valid
        if(arrSQLTerms.length == 0)
            throw new DBAppException("Invalid SQLTerms");
        if(strarrOperators.length != arrSQLTerms.length - 1)
            throw new DBAppException("Invalid operators");

        //get the table from allTables
        String strTableName = arrSQLTerms[0]._strTableName;
        Table t = null;
        for(String x:allTables){
            if(x==strTableName)
                try {
                    FileInputStream fileIn = new FileInputStream(strTableName + "_table");
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    t = (Table) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DBAppException(e.toString());
                } catch (ClassNotFoundException c) {
                    c.printStackTrace();
                    throw new DBAppException(c.toString());
                }
        }
        //if(t==null)
        //    throw new DBAppException("This table does not exist.");

        //initiate some useful variables
        String clusteringKeyColumn = "";
        Vector<String> columnsInTable = new Vector<String>();
        Vector<String> indexInTable = new Vector<String>();
        String indexNameUsed = "";

        //extract columns and indexes used in the table from metadata.csv
        File file = new File("src/main/resources/metadata.csv");
        try {
            FileReader outputFile = new FileReader(file);
            CSVReader reader = new CSVReader(outputFile);

            while (true) {
                String[] data = reader.readNext();
                if (data == null)
                    break;
                if (data[0].equals(strTableName)) {
                    columnsInTable.add(data[1]);
                    indexInTable.add(data[4]);
                    if(data[3].equals("true")){
                        clusteringKeyColumn = data[1];
                    }
                }
            }
        }
        catch(IOException | CsvValidationException e){
            throw new DBAppException(e.toString());
        }

        //extract columns used in the query and check if the clustering key is one of them
        Vector<String> columnsInQuery = new Vector<String>();
        Boolean clusteringKeyColumnPresent = false;
        for(SQLTerm s: arrSQLTerms){
            columnsInQuery.add(s._strColumnName);
            if(s._strColumnName.equals(clusteringKeyColumn))
                clusteringKeyColumnPresent = true;
        }

        //extract the indexes used for each column in the query
        String[] indexName = new String[columnsInQuery.size()];
        for(int i = 0; i<columnsInQuery.size(); i++){
            for(int j = 0; j<columnsInTable.size(); j++){
                if(columnsInQuery.get(i).equals(columnsInTable.get(j))){
                    indexName[i] = indexInTable.get(j);
                }
            }
        }

        //check if 3 of the columns use the same index
        for(int i = 0; i< columnsInQuery.size(); i++){
            String currIndexName = indexName[i];
            int count = 0;
            for(int j = 0; j<columnsInQuery.size(); j++){
                if(indexName[j].equals(currIndexName) && !(indexName[j].equals("")))
                    count++;
            }
            if(count >= 3)
                indexNameUsed = currIndexName;
        }

        //check if the 3 columns in the index are ANDED together
        int count = 0;
        boolean columnsAreAnded = false;
        for(String s: strarrOperators){
            if(s.equals("AND"))
                count++;
        }
        if(count>=2)
            columnsAreAnded = true;


        Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();

        if(!indexNameUsed.equals("") && columnsAreAnded){
        //***use index for search***

            System.out.println("Search by index was used!");
            Octree<String> octree = getOctree(indexNameUsed);
            SQLTerm[] SQLTermsUsingIndex = new SQLTerm[3];
            for(SQLTerm s : arrSQLTerms){
                if(s._strColumnName.equals(octree.colX))
                    SQLTermsUsingIndex[0] = s;
                if(s._strColumnName.equals(octree.colY))
                    SQLTermsUsingIndex[1] = s;
                if(s._strColumnName.equals(octree.colZ))
                    SQLTermsUsingIndex[2] = s;
            }
            Vector<String> filePath = octree.getIf(SQLTermsUsingIndex);

            //remove duplicates
            for(int j = 0; j< filePath.size(); j++){
                String path = filePath.get(j);
                for(int k = j + 1; k< filePath.size();k++){
                    if(filePath.get(k).equals(path)){
                        filePath.removeElementAt(k);
                        k = j;
                    }
                }
            }

            for(String fp: filePath){
                Page p = null;
                try {

                    FileInputStream fileIn = new FileInputStream(fp);
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    p = (Page) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DBAppException(e.toString());
                } catch (ClassNotFoundException c) {
                    c.printStackTrace();
                    throw new DBAppException(c.toString());
                }
                //linear search over the page
                for (int j = 0; j < p.size(); j++) {
                    Hashtable<String,Object> tuple = p.get(j);
                    if (checkSQLTerms(tuple, arrSQLTerms, strarrOperators)) {
                        result.add(tuple);
                    }
                }
            }

        }
        else {
        //***use linear search***

            System.out.println("Linear search was used!");

            //iterate over the pages one by one (linearly)
            Page p = null;
            for(int i=0;i<t.getAllPages().size();i++) {

                //read the file and put it in variable "p"
                while (!t.getAllPages().contains(strTableName + String.valueOf(i))) {
                    i++;
                    if(t.lastPage.equals(strTableName+(i-1)))
                        break;
                }
                p = getPage(strTableName, i);

                //linear search over the page
                for (int j = 0; j < p.size(); j++) {
                    Hashtable<String,Object> tuple = p.get(j);
                    if (checkSQLTerms(tuple, arrSQLTerms, strarrOperators)) {
                        result.add(tuple);
                    }
                }
            }
        }
        return result.iterator();
    }

    public static boolean checkSQLTerms(Hashtable<String,Object> tuple, SQLTerm[] arrSQLTerms, String[] strarrOperators){

        if(arrSQLTerms.length==1){
            return arrSQLTerms[0].compare(tuple.get(arrSQLTerms[0]._strColumnName));
        }

        Boolean flag = true;

        String o = strarrOperators[0];
        String column1 = arrSQLTerms[0]._strColumnName;
        String column2 = arrSQLTerms[1]._strColumnName;

        if(o.equals("AND")){
            if(arrSQLTerms[0].compare(tuple.get(column1))
               && arrSQLTerms[1].compare(tuple.get(column2))) {
                //do nothing
            }
            else {
                flag = false;
            }
        } else if (o.equals("OR")){
            if(arrSQLTerms[0].compare(tuple.get(column1))
               || arrSQLTerms[1].compare(tuple.get(column2))) {
                //do nothing
            }
            else {
                flag = false;

            }
        } else if (o.equals("XOR")){
            if(arrSQLTerms[0].compare(tuple.get(column1))
               ^ arrSQLTerms[1].compare(tuple.get(column2))) {
                //do nothing
            }
            else {
                flag = false;

            }
        }

        for(int i = 1; i<strarrOperators.length; i++){
            o = strarrOperators[i];
            column2 = arrSQLTerms[i+1]._strColumnName;

            if(o.equals("AND")){
                if(flag
                   && arrSQLTerms[i+1].compare(tuple.get(column2))) {
                    flag = true;
                }
                else {
                    flag = false;
                }
            } else if (o.equals("OR")){
                if(flag
                   || arrSQLTerms[i+1].compare(tuple.get(column2))) {
                    flag = true;
                }
                else {
                    flag = false;
                }
            } else if (o.equals("XOR")){
                if(flag
                   ^ arrSQLTerms[i+1].compare(tuple.get(column2))) {
                    flag = true;
                }
                else {
                    flag = false;
                }
            }
        }
        return flag;
    }

    private static void  insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");


            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

    private static void  insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }
    private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }
    private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }
    private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }
    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }

    public static void main(String[] args) throws Exception {
        DBApp db = new DBApp();
        db.init();

        /*SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "first_name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue =row.get("first_name");

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "<=";
        arrSQLTerms[1]._objValue = row.get("gpa");

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "OR";
        String table = "students";

        row.put("first_name", "fooooo");
        row.put("last_name", "baaaar");

        Date dob = new Date(1992 - 1900, 9 - 1, 8);
        row.put("dob", dob);
        row.put("gpa", 1.1);

        db.updateTable(table, clusteringKey, row);*/

        createCoursesTable(db);
        createPCsTable(db);
        createTranscriptsTable(db);
        createStudentTable(db);
        //insertPCsRecords(db,200);
        //insertTranscriptsRecords(db,200);
        //insertStudentRecords(db,50);
        //insertCoursesRecords(db,200);

        String strTableName = "students";
        String table = "students";
        /*Hashtable<String, Object> row = new Hashtable();
        row.put("id", "43-5679");

        row.put("id", "62-0353");
        row.put("last_name", "Sadek");

        Date dob = new Date(1995 - 1900, 4 - 1, 1);
        row.put("dob", dob);
        row.put("gpa", 1.1);
        db.updateTable(table,"46-7036",row);*/

        db.createIndex(  table,new String[]{"first_name","last_name","gpa"});

        /*for(int i=0;i<1;i++) {
                Page p = db.getPage(strTableName, i);
                for(Hashtable<String,Object> t:p)
                    System.out.println(t);
        }*/

        /*Hashtable<String, Object> row = new Hashtable();
        row.put("id", "43-5678");
        db.deleteFromTable(table,row);*/

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[3];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "first_name";
        arrSQLTerms[0]._strOperator = "<=";
        arrSQLTerms[0]._objValue = "mmmmm";

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "last_name";
        arrSQLTerms[1]._strOperator = "<=";
        arrSQLTerms[1]._objValue = "kkkkk";

        arrSQLTerms[2] = new SQLTerm();
        arrSQLTerms[2]._strTableName = "students";
        arrSQLTerms[2]._strColumnName= "gpa";
        arrSQLTerms[2]._strOperator = "<=";
        arrSQLTerms[2]._objValue = 2.5;

        String[]strarrOperators = new String[2];
        strarrOperators[0] = "AND";
        strarrOperators[1] = "AND";

        //SELECT PRINT
        Iterator iter = db.selectFromTable(arrSQLTerms,strarrOperators); //SELECT PRINT
        while(iter.hasNext())
            System.out.println(iter.next());
        System.out.println(" ");

        Hashtable<String, Object> row = new Hashtable();
        //row.put("id", "43-8888");
        //row.put("first_name", "nora");
        row.put("last_name", "sadekkkk");
        Date dob = new Date(1995 - 1900, 4 - 1, 1);
        //row.put("dob", dob);
        //row.put("gpa", 1.1);
        db.deleteFromTable(table,row);

        //db.deleteFromTable(table,row);

        //OCTREE PRINT
        Octree<String> oct = db.getOctree("first_namelast_namegpaIndex"); //OCTREE PRINT
        int level = 0;
        boolean levelIncrease = false;
        ArrayList<Octree<String>> q = new ArrayList<Octree<String>>();
        q.add(oct);
        System.out.println("PRINTING OCTREE...");
        while(!q.isEmpty()){
            Octree<String> curr = q.remove(0);
            System.out.println("LEVEL " + level + " START ***");
            for(int i = 0; i<8; i++){
                if(curr.children[i].point == null){
                    q.add(curr.children[i]);
                    System.out.println("[V NON-LEAF V], ");
                    if(!levelIncrease) {
                        level++;
                        levelIncrease = true;
                    }
                } else {
                    System.out.println("NODE * MIN: " + curr.children[i].topLeftFront + " MAX: " + curr.children[i].bottomRightBack);
                    System.out.print("[");
                    for(int j = 0; j< curr.children[i].point.size(); j++)
                        System.out.print(curr.children[i].point.get(j).toString() + " in " + curr.children[i].object.get(j) + ", ");
                    System.out.println("], \n");
                }
            }
            levelIncrease = false;
            System.out.println("LEVEL END ***");
            System.out.println(" ");
        }
    }
}
