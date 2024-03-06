import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {

    private Vector<String> allPages; //vector (list) of all pages associated with the table
    private String tableName;
    public String lastPage;

    public Table(String tableName){
        this.allPages = new Vector<String>();
        this.tableName = tableName;
    }

    //creates a page
    //this should include turning it into a file
    public void createPage(String filePath) throws DBAppException {
        Page p = new Page(filePath);
        allPages.add(filePath);
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
            //System.out.println("Serialized data is saved in " + filePath);
        } catch (IOException i) {
            i.printStackTrace();
            throw new DBAppException(i.toString());
        }
        lastPage = filePath;
    }

    //deletes a page
    //this should include deleting the file
    public void deletePage(Page page){
        File pageFile = new File(page.filePath);
        pageFile.delete();
        allPages.remove(page);
        if(page.filePath.equals(lastPage)){
            lastPage = allPages.lastElement();
        }
    }

    public String getTableName(){
        return tableName;
    }

    public Vector<String> getAllPages() {
        return allPages;
    }
}
