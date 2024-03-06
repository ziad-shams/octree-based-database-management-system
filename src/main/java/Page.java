import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

//page class which has the same functionality as a vector of hashtables
//according to piazza a page should be a vector of hashtables
//both page and table should implement serializable, aka can be turned into a file
public class Page extends Vector<Hashtable<String,Object>> implements Serializable {
    String filePath;
    Page(String filePath){
        super();
        this.filePath = filePath;
    }
}
