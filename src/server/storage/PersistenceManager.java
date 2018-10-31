package server.storage;

import server.app.KVServer;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class PersistenceManager implements IPersistenceManager {


    @Override
    public boolean write(String key, String value) {
        return false;
    }

    private void searchRecordFile(String fileName) {
        try {
            Path dbPath = Paths.get(KVServer.DB_PATH);
            Files.walkFileTree(dbPath, new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String fileString = file.toAbsolutePath().toString();
                    //System.out.println("pathString = " + fileString);

                    if(fileString.endsWith(fileName)){
                        System.out.println("file found at path: " + file.toAbsolutePath());
                        return FileVisitResult.TERMINATE;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public String read(String key) {
        return null;
    }
}
