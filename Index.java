import java.io.Serializable;
//import java.util.HashMap;

public class Index implements Serializable {
    public int chunkSize;
    public int refCount;

    public Index(int chunkSize, int refCount) {
        this.chunkSize = chunkSize;
        this.refCount = refCount;
    }

    public Index() {
      chunkSize = 0;
      refCount = 0;
    }

    // @Override
    // public String toString() {
    //     return "MyIndex{" +
    //             "chunkSize=" + chunkSize +
    //             ", refCount=" + refCount +
    //             '}';
    // }
}
