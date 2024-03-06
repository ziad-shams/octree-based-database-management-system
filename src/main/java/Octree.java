import org.apache.commons.lang3.ObjectUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.*;

public class Octree<T> implements Serializable {

    public String colX;
    public String colY;
    public String colZ;

    public static int maxEntries;

    public Vector<OctPoint> point;

    public OctPoint topLeftFront, bottomRightBack;

    public Octree<T>[] children = new Octree[8];

    public Vector<T> object;

    public Octree(String colX, String colY, String colZ){

        point = new Vector<OctPoint>();
        this.colX = colX;
        this.colY = colY;
        this.colZ = colZ;
        this.object = new Vector<>();
    }

    public Octree(Comparable x, Comparable y, Comparable z, T object, String colX, String colY, String colZ){

        if(x.getClass() == String.class){
            x = ((String) x).toLowerCase();
        }
        if(y.getClass() == String.class){
            y = ((String) y).toLowerCase();
        }
        if(z.getClass() == String.class){
            z = ((String) z).toLowerCase();
        }

        if (x==null)
            x = new NullObject();
        if (y==null)
            y = new NullObject();
        if (z==null)
            z = new NullObject();

        point = new Vector<OctPoint>();
        point.add(new OctPoint(x, y, z));
        this.object = new Vector<T>();
        this.object.add(object);

        this.colX = colX;
        this.colY = colY;
        this.colZ = colZ;
    }

    public Octree(Comparable x1, Comparable y1, Comparable z1, Comparable x2, Comparable y2, Comparable z2, String colX, String colY, String colZ) throws  DBAppException {

        if(x1.getClass() == String.class){
            x1 = ((String) x1).toLowerCase();
        }
        if(y1.getClass() == String.class){
            y1 = ((String) y1).toLowerCase();
        }
        if(z1.getClass() == String.class){
            z1 = ((String) z1).toLowerCase();
        }
        if(x2.getClass() == String.class){
            x2 = ((String) x2).toLowerCase();
        }
        if(y2.getClass() == String.class){
            y2 = ((String) y2).toLowerCase();
        }
        if(z2.getClass() == String.class){
            z2 = ((String) z2).toLowerCase();
        }

        //load DBApp.config
        try (InputStream input = new FileInputStream("src/main/resources/DBApp.config")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            maxEntries = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));

        } catch (IOException ex) {
            ex.printStackTrace();
            throw new DBAppException(ex.toString());
        }

        if(x2.compareTo(x1) < 0 || y2.compareTo(y1) < 0 || z2.compareTo(z2) < 0){
            throw new DBAppException("The bounds are not properly set!");
        }

        point = null;
        topLeftFront = new OctPoint(x1, y1, z1);
        bottomRightBack = new OctPoint(x2, y2, z2);

        for (int i = 0; i <= 7; i++){
            children[i] = new Octree<>(colX, colY, colZ);
        }

        this.colX = colX;
        this.colY = colY;
        this.colZ = colZ;
    }

    private Comparable getMidPoint(Comparable a, Comparable b){
        Comparable mid = null;
        if(a.getClass()==String.class){

            // Stores the base 26 digits after addition
            String a_ = ((String) a) .toLowerCase();
            String b_ = ((String) b) .toLowerCase();
            int N = 10;
            while (a_.length()<N)
                a_ = a_ + " ";
            while (b_.length()<N)
                b_ = b_ + " ";
            int[] a1 = new int[N + 1];
            for (int i = 0; i < N; i++) {
                a1[i + 1] = (int)a_.charAt(i) - 97
                        + (int)b_.charAt(i) - 97;
            }
            // Iterate from right to left
            // and add carry to next position
            for (int i = N; i >= 1; i--) {
                a1[i - 1] += (int)a1[i] / 26;
                a1[i] %= 26;
            }
            // Reduce the number to find the middle
            // string by dividing each position by 2
            for (int i = 0; i <= N; i++) {
                // If current value is odd,
                // carry 26 to the next index value
                if ((a1[i] & 1) != 0) {
                    if (i + 1 <= N) {
                        a1[i + 1] += 26;
                    }
                }
                a1[i] = (int)a1[i] / 2;
            }
            mid = "";
            for (int i = 1; i <= N; i++) {
                mid = ((String) mid) + (char)(a1[i] + 97);
            }

        } else if(a.getClass()==Date.class){

            Date a_ = (Date) a;
            Date b_ = (Date) b;
            LocalDate a__ = a_.toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            LocalDate b__ = b_.toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            Period p = Period.between(a__, b__);
            p = Period.of(p.getYears()/2, p.getMonths()/2, p.getDays()/2);
            LocalDate mid_ = a__.plus(p);
            mid = java.util.Date.from(mid_.atStartOfDay()
                    .atZone(ZoneId.systemDefault())
                    .toInstant());

        } else if(a.getClass()==Double.class){

            double a_ = (double) a;
            double b_ = (double) b;
            mid = (a_ + b_)/2.0;

        } else{

            int a_ = (int) a;
            int b_ = (int) b;
            mid = (a_ + b_)/2;

        }
        return mid;
    }

    private Comparable plusOne(Comparable a){
        Comparable result = null;
        if(a.getClass()==String.class){

            // Stores the base 26 digits after addition
            String a_ = (String) a;
            char[] a_CharArray = a_.toCharArray();
            a_CharArray[a_.length() - 1] = (char) (a_CharArray[a_.length() - 1] + 1);
            //a_ = a_CharArray.toString();
            result = "";
            for(char c: a_CharArray)
                result = (String)result + c;
            result = a_;
        } else if(a.getClass()==Date.class){

            Date a_ = (Date) a;
            a_.setSeconds(a_.getSeconds()+1);
            result = a;

        } else if(a.getClass()==Double.class){

            double a_ = (double) a;
            result = a_ + 0.001;

        } else{

            int a_ = (int) a;
            result = a_ + 1;

        }
        return result;
    }

    public void insert(Comparable x, Comparable y, Comparable z, T object) throws DBAppException {

        if(x.getClass() == String.class){
            x = ((String) x).toLowerCase();
        }
        if(y.getClass() == String.class){
            y = ((String) y).toLowerCase();
        }
        if(z.getClass() == String.class){
            z = ((String) z).toLowerCase();
        }

        if (x==null)
            x = new NullObject();
        if (y==null)
            y = new NullObject();
        if (z==null)
            z = new NullObject();

//        if(find(x, y, z)){
//            throw new DBAppException("Point already exists in the tree. X: " + x + " Y: " + y + " Z: " + z + " Object Name: " + object.getClass().getName());
//        }

        if (x.compareTo(topLeftFront.getX()) < 0 || x.compareTo(bottomRightBack.getX()) > 0
                || y.compareTo(topLeftFront.getY()) < 0 || y.compareTo(bottomRightBack.getY()) > 0
                || z.compareTo(topLeftFront.getZ()) < 0 || z.compareTo(bottomRightBack.getZ()) > 0){
            throw new DBAppException("Insertion point is out of bounds! X: " + x + " Y: " + y + " Z: " + z + " Object Name: " + object.getClass().getName());
        }

        Comparable midx = getMidPoint(topLeftFront.getX(),bottomRightBack.getX());
        Comparable midy = getMidPoint(topLeftFront.getY(),bottomRightBack.getY());
        Comparable midz = getMidPoint(topLeftFront.getZ(),bottomRightBack.getZ());

        int pos;

        if(x.compareTo(midx) <= 0 ){
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopLeftFront.getNumber();
                else
                    pos = OctLocations.TopLeftBottom.getNumber();
            }else{
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomLeftFront.getNumber();
                else
                    pos = OctLocations.BottomLeftBack.getNumber();
            }
        }else{
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopRightFront.getNumber();
                else
                    pos = OctLocations.TopRightBottom.getNumber();
            }else {
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomRightFront.getNumber();
                else
                    pos = OctLocations.BottomRightBack.getNumber();
            }
        }

        if(children[pos].point == null){
            children[pos].insert(x, y, z, object);
        }
        else if(children[pos].object == null || children[pos].object.isEmpty()){
            children[pos] = new Octree<>(x, y, z, object, colX, colY, colZ);
            if(pos == OctLocations.TopLeftFront.getNumber()){
                children[pos].topLeftFront = new OctPoint(topLeftFront.getX(), topLeftFront.getY(), topLeftFront.getZ());
                children[pos].bottomRightBack = new OctPoint(midx, midy, midz);
            }
            else if(pos == OctLocations.TopRightFront.getNumber()){
                children[pos].topLeftFront = new OctPoint(plusOne(midx), topLeftFront.getY(), topLeftFront.getZ());
                children[pos].bottomRightBack = new OctPoint(bottomRightBack.getX(), midy, midz);
            }
            else if(pos == OctLocations.BottomRightFront.getNumber()){
                children[pos].topLeftFront = new OctPoint(plusOne(midx), plusOne(midy), topLeftFront.getZ());
                children[pos].bottomRightBack = new OctPoint(bottomRightBack.getX(), bottomRightBack.getY(), midz);
            }
            else if(pos == OctLocations.BottomLeftFront.getNumber()){
                children[pos].topLeftFront = new OctPoint(topLeftFront.getX(), plusOne(midy), topLeftFront.getZ());
                children[pos].bottomRightBack = new OctPoint(midx, bottomRightBack.getY(), midz);
            }
            else if(pos == OctLocations.TopLeftBottom.getNumber()){
                children[pos].topLeftFront = new OctPoint(topLeftFront.getX(), topLeftFront.getY(), plusOne(midz));
                children[pos].bottomRightBack = new OctPoint(midx, midy, bottomRightBack.getZ());
            }
            else if(pos == OctLocations.TopRightBottom.getNumber()){
                children[pos].topLeftFront = new OctPoint(plusOne(midx), topLeftFront.getY(), plusOne(midz));
                children[pos].bottomRightBack = new OctPoint(bottomRightBack.getX(), midy, bottomRightBack.getZ());
            }
            else if(pos == OctLocations.BottomRightBack.getNumber()){
                children[pos].topLeftFront = new OctPoint(plusOne(midx), plusOne(midy), plusOne(midz));
                children[pos].bottomRightBack = new OctPoint(bottomRightBack.getX(), bottomRightBack.getY(), bottomRightBack.getZ());
            }
            else if(pos == OctLocations.BottomLeftBack.getNumber()){
                children[pos].topLeftFront = new OctPoint(topLeftFront.getX(), plusOne(midy), plusOne(midz));
                children[pos].bottomRightBack = new OctPoint(midx, bottomRightBack.getY(), bottomRightBack.getZ());
            }
        }
        else{
            Comparable x_ = children[pos].point.get(0).getX();
            Comparable y_ = children[pos].point.get(0).getY();
            Comparable z_ = children[pos].point.get(0).getZ();
            Vector<OctPoint> point_ = children[pos].point;
            Vector<T> object_ = children[pos].object;
            if(object_.size()<maxEntries){
                object_.add(object);
                point_.add(new OctPoint(x, y, z));
                return;
            }
            children[pos] = null;
            if(pos == OctLocations.TopLeftFront.getNumber()){
                children[pos] = new Octree<>(topLeftFront.getX(), topLeftFront.getY(), topLeftFront.getZ(), midx, midy, midz , colX, colY, colZ);
            }
            else if(pos == OctLocations.TopRightFront.getNumber()){
                children[pos] = new Octree<>(plusOne(midx), topLeftFront.getY(), topLeftFront.getZ(), bottomRightBack.getX(), midy, midz, colX, colY, colZ);
            }
            else if(pos == OctLocations.BottomRightFront.getNumber()){
                children[pos] = new Octree<>(plusOne(midx), plusOne(midy), topLeftFront.getZ(), bottomRightBack.getX(), bottomRightBack.getY(), midz, colX, colY, colZ);
            }
            else if(pos == OctLocations.BottomLeftFront.getNumber()){
                children[pos] = new Octree<>(topLeftFront.getX(), plusOne(midy), topLeftFront.getZ(), midx, bottomRightBack.getY(), midz, colX, colY, colZ);
            }
            else if(pos == OctLocations.TopLeftBottom.getNumber()){
                children[pos] = new Octree<>(topLeftFront.getX(), topLeftFront.getY(), plusOne(midz), midx, midy, bottomRightBack.getZ(), colX, colY, colZ);
            }
            else if(pos == OctLocations.TopRightBottom.getNumber()){
                children[pos] = new Octree<>(plusOne(midx), topLeftFront.getY(), plusOne(midz), bottomRightBack.getX(), midy, bottomRightBack.getZ(), colX, colY, colZ);
            }
            else if(pos == OctLocations.BottomRightBack.getNumber()){
                children[pos] = new Octree<>(plusOne(midx), plusOne(midy), plusOne(midz), bottomRightBack.getX(), bottomRightBack.getY(), bottomRightBack.getZ(), colX, colY, colZ);
            }
            else if(pos == OctLocations.BottomLeftBack.getNumber()){
                children[pos] = new Octree<>(topLeftFront.getX(), plusOne(midy), plusOne(midz), midx, bottomRightBack.getY(), bottomRightBack.getZ(), colX, colY, colZ);
            }
            int k = 0;
            while(!object_.isEmpty()) {
                children[pos].insert(point_.get(k).getX(), point_.get(k).getY(), point_.get(k).getZ(), object_.remove(0));
                k++;
            }
            children[pos].insert(x, y, z, object);
        }
    }

    public boolean find(Comparable x, Comparable y, Comparable z){

        if(x.getClass() == String.class){
            x = ((String) x).toLowerCase();
        }
        if(y.getClass() == String.class){
            y = ((String) y).toLowerCase();
        }
        if(z.getClass() == String.class){
            z = ((String) z).toLowerCase();
        }

        if (x==null)
            x = new NullObject();
        if (y==null)
            y = new NullObject();
        if (z==null)
            z = new NullObject();

        if (x.compareTo(topLeftFront.getX()) < 0 || x.compareTo(bottomRightBack.getX()) > 0
                || y.compareTo(topLeftFront.getY()) < 0 || y.compareTo(bottomRightBack.getY()) > 0
                || z.compareTo(topLeftFront.getZ()) < 0 || z.compareTo(bottomRightBack.getZ()) > 0) return false;

        Comparable midx = getMidPoint(topLeftFront.getX(),bottomRightBack.getX());
        Comparable midy = getMidPoint(topLeftFront.getY(),bottomRightBack.getY());
        Comparable midz = getMidPoint(topLeftFront.getZ(),bottomRightBack.getZ());

        int pos;

        if(x.compareTo(midx) <= 0 ){
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopLeftFront.getNumber();
                else
                    pos = OctLocations.TopLeftBottom.getNumber();
            }else{
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomLeftFront.getNumber();
                else
                    pos = OctLocations.BottomLeftBack.getNumber();
            }
        }else{
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopRightFront.getNumber();
                else
                    pos = OctLocations.TopRightBottom.getNumber();
            }else {
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomRightFront.getNumber();
                else
                    pos = OctLocations.BottomRightBack.getNumber();
            }
        }

        if(children[pos].point == null)
            return children[pos].find(x, y, z);
        if(children[pos].object == null || children[pos].object.isEmpty())
            return false;
        return x.equals(children[pos].point.get(0).getX()) && y.equals(children[pos].point.get(0).getY()) && z.equals(children[pos].point.get(0).getZ());

    }

    public T get(Comparable x, Comparable y, Comparable z){

        if(x.getClass() == String.class){
            x = ((String) x).toLowerCase();
        }
        if(y.getClass() == String.class){
            y = ((String) y).toLowerCase();
        }
        if(z.getClass() == String.class){
            z = ((String) z).toLowerCase();
        }

        if (x==null)
            x = new NullObject();
        if (y==null)
            y = new NullObject();
        if (z==null)
            z = new NullObject();

        if (x.compareTo(topLeftFront.getX()) < 0 || x.compareTo(bottomRightBack.getX()) > 0
                || y.compareTo(topLeftFront.getY()) < 0 || y.compareTo(bottomRightBack.getY()) > 0
                || z.compareTo(topLeftFront.getZ()) < 0 || z.compareTo(bottomRightBack.getZ()) > 0) return null;

        Comparable midx = getMidPoint(topLeftFront.getX(),bottomRightBack.getX());
        Comparable midy = getMidPoint(topLeftFront.getY(),bottomRightBack.getY());
        Comparable midz = getMidPoint(topLeftFront.getZ(),bottomRightBack.getZ());

        int pos;

        if(x.compareTo(midx) <= 0 ){
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopLeftFront.getNumber();
                else
                    pos = OctLocations.TopLeftBottom.getNumber();
            }else{
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomLeftFront.getNumber();
                else
                    pos = OctLocations.BottomLeftBack.getNumber();
            }
        }else{
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopRightFront.getNumber();
                else
                    pos = OctLocations.TopRightBottom.getNumber();
            }else {
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomRightFront.getNumber();
                else
                    pos = OctLocations.BottomRightBack.getNumber();
            }
        }

        if(children[pos].point == null)
            return children[pos].get(x, y, z);
        if(children[pos].object == null  || children[pos].object.isEmpty())
            return null;
        if(x.compareTo(children[pos].point.get(0).getX()) == 0 && y.equals(children[pos].point.get(0).getY()) && z.equals(children[pos].point.get(0).getZ())){
            return children[pos].object.get(0);
        }

        return null;
    }

    public Vector<T> getIf(SQLTerm[] arrSQLTerms){

        Vector<T> resultSet = new Vector<T>();

        for(SQLTerm s:arrSQLTerms){
            if(s._objValue.getClass()==String.class)
                s._objValue = s._objValue.toString().toLowerCase();
        }

        //reorder SQLTerm array in the same order as the octree columns
        SQLTerm[] arrSQLTermsArranged = new SQLTerm[3];
        for(SQLTerm s: arrSQLTerms){
            if(s._strColumnName.equals(colX))
                arrSQLTermsArranged[0] = s;
            else if(s._strColumnName.equals(colY))
                arrSQLTermsArranged[1] = s;
            else if(s._strColumnName.equals(colZ))
                arrSQLTermsArranged[2] = s;
        }
        arrSQLTerms = arrSQLTermsArranged;

        Comparable x = (Comparable) arrSQLTerms[0]._objValue;
        Comparable y = (Comparable) arrSQLTerms[1]._objValue;
        Comparable z = (Comparable) arrSQLTerms[2]._objValue;

        if (x==null)
            x = new NullObject();
        if (y==null)
            y = new NullObject();
        if (z==null)
            z = new NullObject();

        Comparable midx = getMidPoint(topLeftFront.getX(),bottomRightBack.getX());
        Comparable midy = getMidPoint(topLeftFront.getY(),bottomRightBack.getY());
        Comparable midz = getMidPoint(topLeftFront.getZ(),bottomRightBack.getZ());

        Vector<OctLocations> pos = new Vector<OctLocations>();

        if(x.compareTo(midx) <= 0){
            if(arrSQLTerms[0]._strOperator.equals(">=") || arrSQLTerms[0]._strOperator.equals(">") || arrSQLTerms[0]._strOperator.equals("!=") || x instanceof  NullObject){
                pos.add(OctLocations.TopLeftFront);
                pos.add(OctLocations.TopLeftBottom);
                pos.add(OctLocations.BottomLeftFront);
                pos.add(OctLocations.BottomLeftBack);
                pos.add(OctLocations.TopRightFront);
                pos.add(OctLocations.TopRightBottom);
                pos.add(OctLocations.BottomRightFront);
                pos.add(OctLocations.BottomRightBack);
            }
            else if(arrSQLTerms[0]._strOperator.equals("<=") || arrSQLTerms[0]._strOperator.equals("<") || arrSQLTerms[0]._strOperator.equals("=")) {
                pos.add(OctLocations.TopLeftFront);
                pos.add(OctLocations.TopLeftBottom);
                pos.add(OctLocations.BottomLeftFront);
                pos.add(OctLocations.BottomLeftBack);
            }
        }else{
            if(arrSQLTerms[0]._strOperator.equals("<=") || arrSQLTerms[0]._strOperator.equals("<") || arrSQLTerms[0]._strOperator.equals("!=") || x instanceof NullObject) {
                pos.add(OctLocations.TopLeftFront);
                pos.add(OctLocations.TopLeftBottom);
                pos.add(OctLocations.BottomLeftFront);
                pos.add(OctLocations.BottomLeftBack);
                pos.add(OctLocations.TopRightFront);
                pos.add(OctLocations.TopRightBottom);
                pos.add(OctLocations.BottomRightFront);
                pos.add(OctLocations.BottomRightBack);
            } else if (arrSQLTerms[0]._strOperator.equals(">=") || arrSQLTerms[0]._strOperator.equals(">") || arrSQLTerms[0]._strOperator.equals("=")) {
                pos.add(OctLocations.TopRightFront);
                pos.add(OctLocations.TopRightBottom);
                pos.add(OctLocations.BottomRightFront);
                pos.add(OctLocations.BottomRightBack);
            }
        }

        if(y.compareTo(midy) <= 0){
            if(arrSQLTerms[1]._strOperator.equals(">=") || arrSQLTerms[1]._strOperator.equals(">") || arrSQLTerms[1]._strOperator.equals("!=") || y instanceof NullObject) {
                //do nothing
            } else if (arrSQLTerms[1]._strOperator.equals("<=") || arrSQLTerms[1]._strOperator.equals("<") || arrSQLTerms[1]._strOperator.equals("=")){
                pos.remove(OctLocations.BottomRightFront);
                pos.remove(OctLocations.BottomLeftFront);
                pos.remove(OctLocations.BottomRightBack);
                pos.remove(OctLocations.BottomLeftBack);
            }
        }else{
            if(arrSQLTerms[1]._strOperator.equals("<=") || arrSQLTerms[1]._strOperator.equals("<") || arrSQLTerms[1]._strOperator.equals("!=") || y instanceof NullObject) {
                //do nothing
            } else if (arrSQLTerms[1]._strOperator.equals(">=") || arrSQLTerms[1]._strOperator.equals(">") || arrSQLTerms[1]._strOperator.equals("=")){
                pos.remove(OctLocations.TopRightFront);
                pos.remove(OctLocations.TopLeftFront);
                pos.remove(OctLocations.TopRightBottom);
                pos.remove(OctLocations.TopLeftBottom);
            }
        }

        if(z.compareTo(midz) <= 0){
            if (arrSQLTerms[2]._strOperator.equals(">=") || arrSQLTerms[2]._strOperator.equals(">") || arrSQLTerms[2]._strOperator.equals("!=") || z instanceof NullObject){
                //do nothing
            } else if(arrSQLTerms[2]._strOperator.equals("<=") || arrSQLTerms[2]._strOperator.equals("<") || arrSQLTerms[2]._strOperator.equals("=")) {
                pos.remove(OctLocations.TopLeftBottom);
                pos.remove(OctLocations.TopRightBottom);
                pos.remove(OctLocations.BottomLeftBack);
                pos.remove(OctLocations.BottomRightBack);
            }
        }else{
            if(arrSQLTerms[2]._strOperator.equals("<=") || arrSQLTerms[2]._strOperator.equals("<") || arrSQLTerms[2]._strOperator.equals("!=") || z instanceof NullObject) {
                //do nothing
            } else if (arrSQLTerms[2]._strOperator.equals(">=") || arrSQLTerms[2]._strOperator.equals(">") || arrSQLTerms[2]._strOperator.equals("=")){
                pos.remove(OctLocations.TopLeftFront);
                pos.remove(OctLocations.TopRightFront);
                pos.remove(OctLocations.BottomLeftFront);
                pos.remove(OctLocations.BottomRightFront);
            }
        }

        for(OctLocations p : pos) {
            int position = p.getNumber();

            if (children[position].point == null)
                resultSet.addAll(children[position].getIf(arrSQLTerms));
            else if(children[position].object == null || children[position].object.isEmpty()) {
                //do nothing
            }
            else {
                for(int k = 0; k<children[position].object.size(); k++)
                    resultSet.add(children[position].object.get(k));
            }

        }
        return resultSet;
    }

    public boolean remove(Comparable x, Comparable y, Comparable z){

        if(x.getClass() == String.class){
            x = ((String) x).toLowerCase();
        }
        if(y.getClass() == String.class){
            y = ((String) y).toLowerCase();
        }
        if(z.getClass() == String.class){
            z = ((String) z).toLowerCase();
        }

        if (x.compareTo(topLeftFront.getX()) < 0 || x.compareTo(bottomRightBack.getX()) > 0
                || y.compareTo(topLeftFront.getY()) < 0 || y.compareTo(bottomRightBack.getY()) > 0
                || z.compareTo(topLeftFront.getZ()) < 0 || z.compareTo(bottomRightBack.getZ()) > 0) return false;

        Comparable midx = getMidPoint(topLeftFront.getX(),bottomRightBack.getX());
        Comparable midy = getMidPoint(topLeftFront.getY(),bottomRightBack.getY());
        Comparable midz = getMidPoint(topLeftFront.getZ(),bottomRightBack.getZ());

        int pos;

        if(x.compareTo(midx) <= 0 ){
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopLeftFront.getNumber();
                else
                    pos = OctLocations.TopLeftBottom.getNumber();
            }else{
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomLeftFront.getNumber();
                else
                    pos = OctLocations.BottomLeftBack.getNumber();
            }
        }else{
            if(y.compareTo(midy) <= 0){
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.TopRightFront.getNumber();
                else
                    pos = OctLocations.TopRightBottom.getNumber();
            }else {
                if(z.compareTo(midz) <= 0)
                    pos = OctLocations.BottomRightFront.getNumber();
                else
                    pos = OctLocations.BottomRightBack.getNumber();
            }
        }

        if(children[pos].point == null)
            return children[pos].remove(x, y, z);
        if(children[pos].object == null || children[pos].object.isEmpty())
            return false;
        for(int k = 0; k<children[pos].point.size(); k++) {
            if (x.equals(children[pos].point.get(k).getX()) && y.equals(children[pos].point.get(k).getY()) && z.equals(children[pos].point.get(k).getZ())) {
                children[pos].object.remove(k);
                children[pos].point.remove(k);
                break;
            }
        }
        return false;
    }

    public static void main(String[] args) throws DBAppException {
        Octree<String> octree = new Octree<String>(0.0, "A", 0, 99.0, "ZZZZZZZZZZ", 9999999, "colX", "colY", "colZ");
        octree.insert(1.0,"Ahmed Noor",3, "Stunotreallydent0");
        octree.insert(1.0,"Dalia Noor",3387123, "Studnotreallyent1");
        Octree<String> octree2 = new Octree<String>(0.0, new Date(2000, 8, 1), 0, 99.0, new Date(2023, 8, 1), 9999999, "colX", "colY", "colZ");
        octree2.insert(1.0,new Date(2001, 8, 1),3, "Studnotreallyent0");
        octree2.insert(1.0,new Date(2001, 8, 2),3387123, "Studnotreallyent1");
        System.out.println(octree.find(1.0 , "Dalia Noor", 3387123));
        System.out.println(octree.get(1.0 , "Dalia Noor", 3387123));
        System.out.println(octree2.find(1.0 , new Date(2001, 8, 1), 3));
        System.out.println(octree2.get(1.0 , new Date(2001, 8, 2), 3387123));
        System.out.println(octree.getMidPoint("Dalia Noor", "Ahmed Noor"));
    }
}
