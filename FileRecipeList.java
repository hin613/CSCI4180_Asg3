import java.io.Serializable;
//import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FileRecipeList implements Serializable{
    public HashMap<String, List<String>> fileRecipes;

    public FileRecipeList() {
        this.fileRecipes = new HashMap<>();
    }

    // @Override
    // public String toString() {
    //     return "MyFileRecipeList{" +
    //             "fileRecipes=" + fileRecipes +
    //             '}';
    // }
}
