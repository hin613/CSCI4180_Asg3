import java.io.*;
import java.util.*;
import java.math.*;
import java.lang.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Local {

    // public Local() {
    //   File data = new File("data");
    //   if (!data.exists()) {
    //     data.mkdir();
    //   }
    // }

    public static int exp_mod(int d, int exp, int q) {
      int result = 1;
      d = d % q;
      while (exp > 0) {
        if ((exp & 1) > 0) {
          result = (result * d) % q;
        }
        d = ((int) Math.pow(d, 2)) % q;
        exp = exp >> 1;
      }
      return result;
    }

    public static boolean fExists (File fileName) {
      if (fileName.exists()) {
        return true;
      } else {
        System.out.println("File \"" + fileName.getName() + "\" not exist.");
        // System.out.println("Try again!");
        return false;
      }
    }

    public static boolean fMkdir (File fileName) {
      if (fileName.mkdir()) {
        return true;
      } else {
        System.out.println("[ERROR] Cannot create directory: \"" + fileName.getName() + "\"!");
        return false;
      }
    }

    public static boolean fIsDirectory (File fileName) {
      if (fileName.isDirectory()) {
        return true;
      } else {
        System.out.println("[ERROR] \"" + fileName.getName() + "\" is NOT a directory!");
        return false;
      }
    }

    public static void closeInputStream (FileInputStream inputFile, ObjectInputStream inputObj)
      throws IOException {
        inputObj.close();
        inputFile.close();
    }

    public static void closeOutputStream (FileOutputStream outputFile, ObjectOutputStream outputObj)
      throws IOException {
        outputObj.close();
        outputFile.close();
    }

    public void upload(int minChunk, int avgChunk, int maxChunk, int d, String fileToUpload, String storageType)
      throws IOException, NoSuchAlgorithmException, ClassNotFoundException, NullPointerException {
        File file = new File(fileToUpload);
        if (!fExists(file)) {
          System.out.println("[ERROR] Upload file NOT exists! Try again!");
          return;
        }


        long fileSize = file.length();

        FileInputStream uploadFile = new FileInputStream(file);
        FileRecipeList FileRecipeList = new FileRecipeList();
        List<String> fileRecipe = new ArrayList<>();
        IndexList IndexList = new IndexList();

        //for local
        FileOutputStream indexFileOut = null;
        ObjectOutputStream indexObjOut = null;
        FileOutputStream recipesFileOut = null;
        ObjectOutputStream recipesObjOut = null;


        File dir = new File("data");

        // error handle
        if (!fExists(dir)) {
          if (!fMkdir(dir)) {
            // System.out.println("[ERROR] Cannot create directory: \"data\"");
            return;
          }
        }

        if (!fIsDirectory(dir)) {
          // System.out.println("[ERROR] \"data\": not a directory");
          return;
        }


        FileInputStream fileIn;
        ObjectInputStream objIn;
        File indexFile;
        File recipesFile;

        // index file
        indexFile = new File(dir.getName() + "/mydedup.index");
        // if (!indexFile.exists()) {
        if (!fExists(indexFile)) {
          System.out.println("Creating index file...");
          indexFile.createNewFile();
        } else {
          System.out.println("Index file exisits.");

          fileIn = new FileInputStream(indexFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);

          IndexList = (IndexList) objIn.readObject();

          closeInputStream(fileIn, objIn);
          // objIn.close();
          // fileIn.close();
        }
        indexFileOut = new FileOutputStream(indexFile.getAbsolutePath());
        indexObjOut = new ObjectOutputStream(indexFileOut);


        // receipes file
        recipesFile = new File(dir.getName() + "/fileRecipes.index");
        if (!fExists(recipesFile)) {
          System.out.println("Creating recipes file...");
          recipesFile.createNewFile();
        } else {
          System.out.println("Recipes file exisits.");

          fileIn = new FileInputStream(recipesFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          FileRecipeList = (FileRecipeList) objIn.readObject();

          closeInputStream(fileIn, objIn);
          // objIn.close();
          // fileIn.close();
        }

        // System.out.println(FileRecipeList.fileRecipes);

        if (FileRecipeList.fileRecipes.containsKey(fileToUpload)) {
            System.out.println("[ERROR] File already exists!");
        }
        recipesFileOut = new FileOutputStream(recipesFile.getAbsolutePath());
        recipesObjOut = new ObjectOutputStream(recipesFileOut);


        int maxBufferSize = (int) Runtime.getRuntime().freeMemory();
        int chunkIterations = (int) Math.ceil(((float)fileSize / (float)maxBufferSize));
        int lastChunkSize = (int) (fileSize % maxBufferSize);

        long totalLogicChunks = 0;
        long totalUniqueChunks = 0;
        long totalLogicFileBytes = 0;
        long totalUniqueFileBytes = 0;
        double spaceSaving = 0;

        // System.out.println("-----------------------------");
        for (int interation = 0; interation < chunkIterations; interation++) {

            byte[] fileBytes;

            if (interation == chunkIterations - 1) {
              fileBytes = new byte[lastChunkSize];
            } else {
              fileBytes = new byte[maxBufferSize];
            }

            uploadFile.read(fileBytes);

            int fileChunkSize = fileBytes.length;
            // System.out.println(interation);


            int m = minChunk;
            int q = avgChunk;
            int s = 0;
            int currentChunkSize = 0;
            int currentPos = 0;
            int rfp = 0;
            int testingFP = 0;

            List<Integer> offset = new ArrayList<>();
            List<Integer> chunkSize = new ArrayList<>();

            while ((currentPos + currentChunkSize <= fileChunkSize) && (currentPos + m <= fileChunkSize)) {
              if (s == 0) {
                rfp = 0;
                for (int i = 1; i <= m; i++) {
                  // System.out.println("##################");
                  rfp = (rfp + ( (int)(fileBytes[currentPos + i - 1] & 0xff) * exp_mod(d, m - i, q) )) % q;
                  // System.out.println("currentPos + i - 1: " + (currentPos + i - 1));
                  // System.out.println("fileBytes: " + fileBytes[currentPos + i - 1]);
                  // System.out.println("fileBytes & 0xff: " + (fileBytes[currentPos + i - 1] & 0xff));
                  // System.out.println("m - i: " + (m-i));
                  testingFP += ((int)(fileBytes[currentPos + i - 1] & 0xff) * Math.pow(d, m-i)) % q;
                  // System.out.println("rfp: " + rfp);
                  // System.out.println("tFP: " + testingFP);
                }
                currentChunkSize = m;
              } else {
                rfp = (d * (rfp - exp_mod(d, m - 1, q) * (int) (fileBytes[currentPos + s - 1] & 0xff)) + fileBytes[currentPos + s + m - 1]) % q;
                while (rfp < 0) {
                  rfp += q;
                }
                currentChunkSize++;
              }
              // System.out.println("+++++++ RFP: " + rfp);

              if ((rfp & 0xFF) == 0 || currentChunkSize == maxChunk || currentPos + currentChunkSize >= fileChunkSize) {
                chunkSize.add(currentChunkSize);
                offset.add(currentPos);
                currentPos += currentChunkSize;
                s = 0;
                currentChunkSize = 0;
              } else {
                s++;
              }

          }

          // handle special case: chunk smaller than m left behind
          if (currentPos < fileChunkSize) {
            offset.add(currentPos);
            chunkSize.add(fileChunkSize - currentPos);
          }


          if (offset.size() == chunkSize.size() && offset.size() > 0) {
            int numOfChunks = offset.size();
            // Do some file settings here

            for (int i = 0; i < numOfChunks; i++) {
              MessageDigest md = MessageDigest.getInstance("SHA-256");
              // System.out.println(offset.get(i));
              // System.out.println(chunkSize.get(i));

              // md.update(Arrays.copyOfRange(fileBytes, offset.get(i), offset.get(i) + chunkSize.get(i)));
              for (int j = offset.get(i); j < (offset.get(i) + chunkSize.get(i)); j++) {
                md.update(fileBytes[j]);
              }

              byte[] messageDigest = md.digest();

              BigInteger no = new BigInteger(1, messageDigest);

              String tempHashText = no.toString(16);
              String remainHashText = null;
              String hashtext;

              // while (hashtext.length() < 32) {
              //   hashtext = "0" + hashtext;
              // }
              if (tempHashText.length() < 32) {
                for (int j = 0; j < (32 - tempHashText.length()); j++)
                  remainHashText += "0";

                hashtext = remainHashText.concat(tempHashText);
              } else {
                hashtext = tempHashText;
              }


              if (IndexList.index.containsKey(hashtext)) { // already have that chunk, reuse it
                // IndexList.index.get(hashtext).refCount += 1;
                IndexList.index.get(hashtext).refCount = IndexList.index.get(hashtext).refCount + 1;
                fileRecipe.add(hashtext);
              } else {         // if no entry of this chunk in indexTable, do update, create new chunk
                Index index = new Index();

                index.chunkSize = chunkSize.get(i);

                File newChunk = new File(dir.getAbsolutePath() + "/" + hashtext);
                if (newChunk.createNewFile()) {
                  // new chunk file created
                  FileOutputStream tmpOs = new FileOutputStream(newChunk);
                  tmpOs.write(fileBytes, offset.get(i), chunkSize.get(i));
                  tmpOs.close();

                  IndexList.index.put(hashtext, new Index(chunkSize.get(i), 1));
                  fileRecipe.add(hashtext);
                } else {
                  System.out.println("Cannot create chunk file");
                  return;
                }
              }
            }
          }


        }
        // System.out.println("-----------------------------");

        // report




        // for (String key : IndexList.index.keySet()) {
        //   // int refCount = IndexList.index.get(key).refCount;
        //   // int chunkSize = IndexList.index.get(key).chunkSize;
        //   // totalLogicChunks += refCount;
        //   totalUniqueChunks++;
        //   totalLogicChunks += IndexList.index.get(key).refCount;
        //
        //   // totalLogicFileBytes += refCount * chunkSize;
        //   totalLogicFileBytes += IndexList.index.get(key).refCount * IndexList.index.get(key).chunkSize;
        //   // totalUniqueFileBytes += chunkSize;
        //   totalUniqueFileBytes += IndexList.index.get(key).chunkSize;
        // }
        // spaceSaving = 1 - (1.0 * totalUniqueChunks / totalLogicChunks);


        Iterator<String> it = IndexList.index.keySet().iterator();
        while (it.hasNext()) {
          String key = it.next();
          totalUniqueChunks++;
          totalLogicChunks += IndexList.index.get(key).refCount;
          totalUniqueFileBytes += IndexList.index.get(key).chunkSize;
          totalLogicFileBytes += IndexList.index.get(key).refCount * IndexList.index.get(key).chunkSize;
        }
        // spaceSaving = 1 - ((float)totalUniqueChunks / (float)totalLogicChunks);

        System.out.println("Total number of chunks in storage: " + totalLogicChunks);
        System.out.println("Number of unique chunks in storage: " + totalUniqueChunks);
        System.out.println("Number of bytes in storage with deduplication: " + totalUniqueFileBytes);
        System.out.println("Number of bytes in storage without deduplication: " + totalLogicFileBytes);
        System.out.println("Space saving: " + (1 - ((float)totalUniqueChunks / (float)totalLogicChunks)));

        FileRecipeList.fileRecipes.put(fileToUpload, fileRecipe);

        indexObjOut.writeObject(IndexList);
        recipesObjOut.writeObject(FileRecipeList);
        closeOutputStream(indexFileOut, indexObjOut);
        // indexObjOut.close();
        // indexFileOut.close();
        closeOutputStream(recipesFileOut, recipesObjOut);
        // recipesObjOut.close();
        // recipesFileOut.close();
    }




    public void download(String fileToDownload, String pathName, String storageType) throws IOException {
      if (!fIsDirectory(new File(pathName))) return;

      try {
        String downloadedFileName = "download_" + fileToDownload;
        FileRecipeList FileRecipeList = new FileRecipeList();
        List<String> fileRecipe = new ArrayList<>();
        IndexList IndexList = new IndexList();


        //for local
        File dir = new File("data");

        // error handle
        if (!fExists(dir)) {
          if (!fMkdir(dir)) return;
        }
        if (!fIsDirectory(dir)) return;


        FileInputStream fileIn;
        ObjectInputStream objIn;
        File indexFile;
        // Boolean isNewIndexFile;
        File recipesFile;
        // Boolean isNewRecipesFile;

        // index file
        indexFile = new File(dir.getName() + "/mydedup.index");

        // isNewIndexFile = indexFile.createNewFile();

        // if (!isNewIndexFile) {
        if (!indexFile.createNewFile()) {
          fileIn = new FileInputStream(indexFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          IndexList = (IndexList) objIn.readObject();
          closeInputStream(fileIn, objIn);
          // objIn.close();
          // fileIn.close();
        }


        // recipe
        recipesFile = new File(dir.getName() + "/fileRecipes.index");
        // Boolean isNewRecipesFile = recipesFile.createNewFile();
        if (!recipesFile.createNewFile()) {
          fileIn = new FileInputStream(recipesFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          FileRecipeList = (FileRecipeList) objIn.readObject();
          closeInputStream(fileIn, objIn);
          // objIn.close();
          // fileIn.close();
        }

        FileInputStream fis;
        byte[] fileBytes;
        int bytesRead = 0;
        FileOutputStream fos = new FileOutputStream(new File(downloadedFileName));
        List<String> recipeList = FileRecipeList.fileRecipes.get(fileToDownload);

        for (String chunkName : recipeList) {
          File file = new File(dir.getName() + "/" + chunkName);
          fis = new FileInputStream(file);
          fileBytes = new byte[(int) file.length()];
          bytesRead = fis.read(fileBytes, 0, (int) file.length());
          fos.write(fileBytes);
          fos.flush();
          fileBytes = null;
          fis.close();
          fis = null;
        }
        fos.close();
        fos = null;
      }
      catch (Exception e) {
        System.out.println("[ERROR] Cannot download");
        System.out.println(e.getMessage());
      }
    }




    public void delete(String fileToDelete, String storageType)
      throws IOException, NoSuchAlgorithmException, ClassNotFoundException {

        try {
          FileRecipeList FileRecipeList = new FileRecipeList();
          List<String> fileRecipe = new ArrayList<>();
          IndexList IndexList = new IndexList();

          FileOutputStream indexFileOut = null;
          ObjectOutputStream indexObjOut = null;
          FileOutputStream recipesFileOut = null;
          ObjectOutputStream recipesObjOut = null;


          //for local
          File dir = new File("data");

          // error handle
          if (!fExists(dir)) {
            if (!fMkdir(dir)) return;
          }

          if (!fIsDirectory(dir)) return;

          FileInputStream fileIn;
          ObjectInputStream objIn;
          File indexFile;
          File recipesFile;

          // index file
          indexFile = new File(dir.getName() + "/mydedup.index");


          if (!fExists(indexFile)) {
            System.out.println("Creating index file...");
            indexFile.createNewFile();
          } else {
            System.out.println("Index file exisits.");
            fileIn = new FileInputStream(indexFile.getAbsolutePath());
            objIn = new ObjectInputStream(fileIn);
            IndexList = (IndexList) objIn.readObject();

            closeInputStream(fileIn, objIn);
            // objIn.close();
            // fileIn.close();
          }

          indexFileOut = new FileOutputStream(indexFile.getAbsolutePath());
          indexObjOut = new ObjectOutputStream(indexFileOut);


          // receipes file
          recipesFile = new File(dir.getName() + "/fileRecipes.index");
          if (!fExists(recipesFile)) {
            System.out.println("Creating recipes file...");
            recipesFile.createNewFile();
          } else {
            System.out.println("Recipes file exisits.");
            fileIn = new FileInputStream(recipesFile.getAbsolutePath());
            objIn = new ObjectInputStream(fileIn);
            FileRecipeList = (FileRecipeList) objIn.readObject();

            closeInputStream(fileIn, objIn);
            // objIn.close();
            // fileIn.close();
          }

          recipesFileOut = new FileOutputStream(recipesFile.getAbsolutePath());
          recipesObjOut = new ObjectOutputStream(recipesFileOut);


          List<String> recipeList = FileRecipeList.fileRecipes.get(fileToDelete);

          for (String hashtext : recipeList) {
            IndexList.index.get(hashtext).refCount -= 1;
            System.out.println(IndexList.index.get(hashtext).refCount);
            System.out.println("refCount -= 1");
            if (IndexList.index.get(hashtext).refCount == 0) {
              IndexList.index.remove(hashtext);

              File file = new File(dir.getName() + "/" + hashtext);
              file.delete();
              System.out.println("file.delete()");
              FileRecipeList.fileRecipes.remove(fileToDelete);
              System.out.println("FileRecipeList.fileRecipes is removed");
            }
          }

          indexObjOut.writeObject(IndexList);
          recipesObjOut.writeObject(FileRecipeList);
          closeOutputStream(indexFileOut, indexObjOut);
          // indexObjOut.close();
          // indexFileOut.close();
          closeOutputStream(recipesFileOut, recipesObjOut);
          // recipesObjOut.close();
          // recipesFileOut.close();

        } catch (Exception e) {
          System.out.println("[ERROR] Cannot delete");
          System.out.println(e.getMessage());
        }
    }

}
