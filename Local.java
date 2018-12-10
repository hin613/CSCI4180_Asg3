import java.io.*;
import java.util.*;
import java.math.*;
import java.lang.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Local {

    public static int baseMod(int d, int exp, int q) {
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
            return;
          }
        }

        if (!fIsDirectory(dir)) {
          return;
        }


        FileInputStream fileIn;
        ObjectInputStream objIn;
        File indexFile;
        File recipesFile;

        // index file
        indexFile = new File(dir.getAbsolutePath() + "/mydedup.index");
        if (!fExists(indexFile)) {
          System.out.println("Creating index file...");
          indexFile.createNewFile();
        } else {
          System.out.println("Index file exisits.");

          fileIn = new FileInputStream(indexFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);

          IndexList = (IndexList) objIn.readObject();

          closeInputStream(fileIn, objIn);
        }
        indexFileOut = new FileOutputStream(indexFile.getAbsolutePath());
        indexObjOut = new ObjectOutputStream(indexFileOut);


        // receipes file
        recipesFile = new File(dir.getAbsolutePath() + "/fileRecipes.index");
        if (!fExists(recipesFile)) {
          System.out.println("Creating recipes file...");
          recipesFile.createNewFile();
        } else {
          System.out.println("Recipes file exisits.");

          fileIn = new FileInputStream(recipesFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          FileRecipeList = (FileRecipeList) objIn.readObject();

          closeInputStream(fileIn, objIn);
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

        for (int interation = 0; interation < chunkIterations; interation++) {

            byte[] fileBytes;

            if (interation == chunkIterations - 1) {
              fileBytes = new byte[lastChunkSize];
            } else {
              fileBytes = new byte[maxBufferSize];
            }

            uploadFile.read(fileBytes);
            int fileChunkSize = fileBytes.length;
            int m = minChunk;
            int q = avgChunk;
            int s = 0;
            int mask = (int) Math.pow(2, m) - 1;

            int currentChunkSize = 0;
            int cursor = 0;
            int rfp = 0;
            int testingFP = 0;

            List<Integer> checkSum = new ArrayList<>();
            List<Integer> chunkSize = new ArrayList<>();

            while ((cursor + currentChunkSize <= fileChunkSize) && (cursor + m <= fileChunkSize)) {
              if (s == 0) {
                rfp = 0;
                for (int i = 1; i <= m; i++) {
                  rfp = (rfp + ( (int)(fileBytes[cursor + i - 1] & 0xff) * baseMod(d, m - i, q) )) % q;
                // testingFP += ((int)(fileBytes[cursor + i - 1] & 0xff) * Math.pow(d, m-i)) % q;
                }
                currentChunkSize = m;
              } else {
                rfp = (d * (rfp - baseMod(d, m - 1, q) * (int) (fileBytes[cursor + s - 1] & 0xff)) + fileBytes[cursor + s + m - 1]) % q;
                while (rfp < 0) {
                  rfp += q;
                }
                currentChunkSize++;
              }

              if ((rfp & 0xFF) == 0 || currentChunkSize == maxChunk || cursor + currentChunkSize >= fileChunkSize) {
                chunkSize.add(currentChunkSize);
                checkSum.add(cursor);
                cursor += currentChunkSize;
                s = 0;
                currentChunkSize = 0;
              } else {
                s++;
              }

          }

          if (cursor < fileChunkSize) {
            checkSum.add(cursor);
            chunkSize.add(fileChunkSize - cursor);
          }


          if (checkSum.size() == chunkSize.size() && checkSum.size() > 0) {
            int numOfChunks = checkSum.size();
            // Do some file settings here

            for (int i = 0; i < numOfChunks; i++) {
              MessageDigest md = MessageDigest.getInstance("SHA-256");

              // md.update(Arrays.copyOfRange(fileBytes, checkSum.get(i), checkSum.get(i) + chunkSize.get(i)));
              for (int j = checkSum.get(i); j < (checkSum.get(i) + chunkSize.get(i)); j++) {
                md.update(fileBytes[j]);
              }

              byte[] messageDigest = md.digest();

              BigInteger no = new BigInteger(1, messageDigest);

              String tempHashText = no.toString(16);
              String remainHashText = null;
              String hashtext;

              if (tempHashText.length() < 32) {
                for (int j = 0; j < (32 - tempHashText.length()); j++)
                  remainHashText += "0";

                hashtext = remainHashText.concat(tempHashText);
              } else {
                hashtext = tempHashText;
              }


              if (IndexList.index.containsKey(hashtext)) {
                // IndexList.index.get(hashtext).refCount += 1;
                IndexList.index.get(hashtext).refCount = IndexList.index.get(hashtext).refCount + 1;
                fileRecipe.add(hashtext);
              } else {
                Index index = new Index();

                index.chunkSize = chunkSize.get(i);

                File newChunk = new File(dir.getAbsolutePath() + "/" + hashtext);
                if (newChunk.createNewFile()) {
                  // new chunk file created
                  FileOutputStream tmpOs = new FileOutputStream(newChunk);
                  tmpOs.write(fileBytes, checkSum.get(i), chunkSize.get(i));
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

        // report
        Iterator<String> it = IndexList.index.keySet().iterator();
        while (it.hasNext()) {
          String key = it.next();
          totalUniqueChunks++;
          totalLogicChunks += IndexList.index.get(key).refCount;
          totalUniqueFileBytes += IndexList.index.get(key).chunkSize;
          totalLogicFileBytes += IndexList.index.get(key).chunkSize * IndexList.index.get(key).refCount;
        }

        System.out.println("Total number of chunks in storage: " + totalLogicChunks);
        System.out.println("Number of unique chunks in storage: " + totalUniqueChunks);
        System.out.println("Number of bytes in storage with deduplication: " + totalUniqueFileBytes);
        System.out.println("Number of bytes in storage without deduplication: " + totalLogicFileBytes);
        System.out.println("Space saving: " + (1 - ((float)totalUniqueChunks / (float)totalLogicChunks)));

        FileRecipeList.fileRecipes.put(fileToUpload, fileRecipe);

        indexObjOut.writeObject(IndexList);
        recipesObjOut.writeObject(FileRecipeList);
        closeOutputStream(indexFileOut, indexObjOut);
        closeOutputStream(recipesFileOut, recipesObjOut);
    }




    public void download(String fileToDownload, String downloadFile, String storageType) throws IOException {

      try {
        String downloadOutputFile = downloadFile;
        FileRecipeList FileRecipeList = new FileRecipeList();
        List<String> fileRecipe = new ArrayList<>();
        IndexList IndexList = new IndexList();


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
        indexFile = new File(dir.getAbsolutePath() + "/mydedup.index");

        if (!indexFile.createNewFile()) {
          fileIn = new FileInputStream(indexFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          IndexList = (IndexList) objIn.readObject();
          closeInputStream(fileIn, objIn);
        }


        // recipe
        recipesFile = new File(dir.getAbsolutePath() + "/fileRecipes.index");
        // Boolean isNewRecipesFile = recipesFile.createNewFile();
        if (!recipesFile.createNewFile()) {
          fileIn = new FileInputStream(recipesFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          FileRecipeList = (FileRecipeList) objIn.readObject();
          closeInputStream(fileIn, objIn);
        }

        FileInputStream dlfis;
        byte[] fileBytes;
        int bytesRead = 0;
        FileOutputStream dlfos = new FileOutputStream(new File(downloadOutputFile));
        List<String> recipeList = FileRecipeList.fileRecipes.get(fileToDownload);

        Iterator<String> it = recipeList.iterator();
        while (it.hasNext()) {
          String chunk = it.next();
          File chunkFile = new File(dir.getAbsolutePath() + "/" + chunk);
          System.out.println(chunkFile.length());
          dlfis = new FileInputStream(chunkFile);
          fileBytes = new byte[(int)chunkFile.length()];
          bytesRead = dlfis.read(fileBytes, 0, (int)chunkFile.length());
          dlfos.write(fileBytes);
          dlfos.flush();
          fileBytes = null;
          dlfis.close();
          dlfis = null;
        }

        dlfos.close();
        dlfos = null;
        System.out.println(downloadOutputFile + " is downloaded.");
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
          indexFile = new File(dir.getAbsolutePath() + "/mydedup.index");


          if (!fExists(indexFile)) {
            System.out.println("Creating index file...");
            indexFile.createNewFile();
          } else {
            System.out.println("Index file exisits.");
            fileIn = new FileInputStream(indexFile.getAbsolutePath());
            objIn = new ObjectInputStream(fileIn);
            IndexList = (IndexList) objIn.readObject();

            closeInputStream(fileIn, objIn);
          }

          indexFileOut = new FileOutputStream(indexFile.getAbsolutePath());
          indexObjOut = new ObjectOutputStream(indexFileOut);


          // receipes file
          recipesFile = new File(dir.getAbsolutePath() + "/fileRecipes.index");
          if (!fExists(recipesFile)) {
            System.out.println("Creating recipes file...");
            recipesFile.createNewFile();
          } else {
            System.out.println("Recipes file exisits.");
            fileIn = new FileInputStream(recipesFile.getAbsolutePath());
            objIn = new ObjectInputStream(fileIn);
            FileRecipeList = (FileRecipeList) objIn.readObject();

            closeInputStream(fileIn, objIn);
          }

          recipesFileOut = new FileOutputStream(recipesFile.getAbsolutePath());
          recipesObjOut = new ObjectOutputStream(recipesFileOut);


          List<String> recipeList = FileRecipeList.fileRecipes.get(fileToDelete);

          Iterator<String> it = recipeList.iterator();
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
          closeOutputStream(recipesFileOut, recipesObjOut);

        } catch (Exception e) {
          System.out.println("[ERROR] Cannot delete");
          System.out.println(e.getMessage());
        }
    }

}
