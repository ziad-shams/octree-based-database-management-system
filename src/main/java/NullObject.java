import org.apache.commons.lang3.ObjectUtils;

import java.io.Serializable;

public class NullObject implements Serializable, Comparable {

    public String toString(){
        return "null";
    }

    public int compareTo(Object o){
        if(o instanceof NullObject)
            return 0;
        else return 0;
    }

    public boolean equals(Object o){
        if(o instanceof NullObject)
            return true;
        else return false;
    }

}
