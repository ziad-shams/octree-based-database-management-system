import java.io.Serializable;

public class OctPoint implements Serializable {

    private Comparable x;
    private Comparable y;
    private Comparable z;

    private boolean nullify = false;

    public OctPoint(Comparable x, Comparable y, Comparable z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public OctPoint(){
        nullify = true;
    }

    public Comparable getX(){
        return x;
    }

    public Comparable getY(){
        return y;
    }

    public Comparable getZ(){
        return z;
    }

    public boolean isNullified(){
        return nullify;
    }

    @Override
    public String toString() {
        return "{" +
                "x=" + x +
                ", y=" + y +
                ", z=" + z +
                '}';
    }
}
