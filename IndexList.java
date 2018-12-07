import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.Arrays;
import java.util.HashMap;

public class IndexList implements Serializable {
    public HashMap<String, Index> index;

    public IndexList() {
        index = new HashMap<String, Index>();
    }

    // @Override
    // public String toString() {
    //     return "MyIndexList{" +
    //             "indexMap=" + indexMap +
    //             '}';
    // }
}
