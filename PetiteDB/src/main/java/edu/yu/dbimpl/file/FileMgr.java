package edu.yu.dbimpl.file;

import edu.yu.dbimpl.config.DBConfiguration;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The file module as a whole is the lowest file in the PetiteDB stack.  It
 * enables higher-levels of the system to view the database as a collection of
 * blocks on disk, where all blocks contain the same number of (API-specified)
 * blocks.  The file module contains methods for random-access reading and
 * writing of blocks.
 *
 * The FileMgr always reads/writes/appends a block-sized number of bytes
 * from/to a file.  The FileMgr ensures that all file operations take place at
 * a block boundary, and ensures that each call to read/write/append incurs
 * exactly one disk access per call.
 *
 * The FileMgr class handles the actual interaction with the OS's file
 * system. Its constructor takes two arguments: a File specifying the root
 * directory of the database and an integer denoting the size of every database
 * block.  The root directory must be created relative to the directory from
 * which the JVM was invoked.  See e.g., https://stackoverflow.com/a/15954821
 * for the difference between user's directory and the current working
 * directory.
 *
 * Clients are forbidden for accessing the file system rooted in the directory
 * supplied to the FileMgr constructor EXCEPT as mediated by a PetiteDB API.
 *
 * Design note: the FileMgr creates files "on demand".  Specifically, if the
 * client invokes an API that references a file, and the file doesn't yet
 * exist, the FileMgr creates a new empty file with that name.
 *
 * Your implementation will likely invoke JDK methods that throw checked
 * exceptions.  As you can see, the API doesn't declare any checked exception:
 * you should catch and rethrow any such exception as a RuntimeException.
 *
 * The DBMS has exactly one FileMgr object (singleton pattern), which is
 * (conceptually) created during system startup, and in practice by a single
 * invocation of the constructor.

 * @author Avraham Leff
 */

public class FileMgr extends FileMgrBase{
    /**
     * The file manager MUST access the DBConfiguration singleton to determine
     * if it is required to manage a brand-new database or to use an existing
     * database.  If the former, the file manager is responsible for
     * (re)initializing the file system to a brand-new state (this is
     * implementation specific).  If the latter, the file manager is responsible
     * to not modify previously persisted state.
     *
     * @param dbDirectory specifies the location of the root database directory
     *                    in which files will be created.  The root directory is the containing
     *                    directory for all database files. If no such directory exists when the
     *                    constructor is invoked, the implementation must create it.
     * @param blocksize
     * @throws IllegalStateException if DBConfiguration cannot supply startup
     *                               information.
     */

    private final File dbDirectory;
    private final int blocksize;
    private final byte[] zeroPage;
    private final Map<String, Object> fileLocks = new ConcurrentHashMap<>();
    private final LRUCache cache;
    private static FileMgr instance;

    public FileMgr(File dbDirectory, int blocksize) {
        super(dbDirectory, blocksize);
        this.blocksize = blocksize;
        this.dbDirectory = dbDirectory;
        this.zeroPage = new byte[blocksize];
        this.cache = new LRUCache(100);
        // Close previous instance if it exists
        if (instance != null) {
            instance.close();
        }
        instance = this;
        boolean isNewDatabase = DBConfiguration.INSTANCE.isDBStartup();
        if(!dbDirectory.exists()){
            boolean created = dbDirectory.mkdirs();
            if(!created){
                throw new RuntimeException("Could not create DB directory");
            }
        }
        File[] files = dbDirectory.listFiles();
        if(files != null && files.length > 0){
            if(isNewDatabase){
                for (File file : files) {
                    boolean worked = file.delete();
                    if (!worked) {
                        throw new RuntimeException("Could not delete file to reset state");
                    }
                }
            }else{
                for(File file : files){
                    this.fileLocks.put(file.getName(), new Object());
                }
            }
        }
    }
    /**
     * Closes all cached RandomAccessFile objects.
     * Should be called during database shutdown.
     */
    public void close() {
        cache.closeAll();
    }


    /** Reads the specified data from disk into main-memory
     *
     * @param blk the disk location
     * @param p the main-memory location
     */
    @Override
    public void read(BlockIdBase blk, PageBase p) {
        if(blk == null){
            throw new IllegalArgumentException("Invalid BlockId");
        }
        fileLocks.putIfAbsent(blk.fileName(), new Object());
        File file = new File(this.dbDirectory, blk.fileName());
        Object lock = this.fileLocks.get(blk.fileName());
        synchronized (lock) {
            if(!file.exists()){
                try {
                    boolean created = file.createNewFile();
                    if (!created) {
                        throw new RuntimeException("Could not create file");
                    }
                    ByteBuffer pageBuffer = ((Page)p).getBuffer();
                    pageBuffer.clear();
                    pageBuffer.put(zeroPage);
                    pageBuffer.rewind();
                    return;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if((int)file.length()/blocksize - 1 < blk.number()){
                ByteBuffer pageBuffer = ((Page)p).getBuffer();
                pageBuffer.clear();
                pageBuffer.put(zeroPage);
                pageBuffer.rewind();
                return;
            }
            try{
                ByteBuffer pageBuffer = ((Page)p).getBuffer();
                pageBuffer.clear();
                RandomAccessFile raf = cache.get(file);
                FileChannel channel = raf.getChannel();
                channel.read(pageBuffer, (long) blk.number() * blocksize);
                pageBuffer.flip();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Writes the specified data from main-memory to disk
     *
     * @param blk the disk location
     * @param p the main-memory location
     */
    @Override
    public void write(BlockIdBase blk, PageBase p) {
        fileLocks.putIfAbsent(blk.fileName(), new Object());
        Object lock = this.fileLocks.get(blk.fileName());
        File file = new File(this.dbDirectory, blk.fileName());
        if (!file.exists()) {
            try {
                boolean created = file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        synchronized (lock){
            try {
                RandomAccessFile raf = cache.get(file);
                if((int)file.length()/blocksize < blk.number()){
                    int space = this.blocksize * (blk.number() - (int)file.length()/blocksize);
                    raf.setLength(raf.length() + space);
                }
                ByteBuffer pageBuffer = ((Page)p).getBuffer();
                pageBuffer.rewind();
                FileChannel channel = raf.getChannel();
                channel.position((long) blk.number() * this.blocksize);
                int bytesWritten = channel.write(pageBuffer);
                if (bytesWritten != this.blocksize) {
                    throw new RuntimeException("Could not write full block");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void addSpace(File file, int space) {
        try{
            RandomAccessFile raf = cache.get(file);
            raf.setLength(raf.length() + space);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /** Allocates a block at the end of the specified file.  The contents of the
     * new block is implementation dependent, but a block of the correct size
     * MUST be written to disk after the call completes.
     *
     * @param filename specifies the file to which the block should be appended
     */
    @Override
    public BlockIdBase append(String filename) {
        fileLocks.putIfAbsent(filename, new Object());
        Object lock = this.fileLocks.get(filename);
        File file = new File(this.dbDirectory, filename);
        BlockIdBase blk;
        try {
            boolean created = file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        synchronized (lock){
            try {
                blk = new BlockId(filename, (int)file.length()/blocksize);
                addSpace(file, this.blocksize);
                return blk;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Return the number of blocks of the specified file.  If the File has not
     * yet been created, returns 0.
     *
     * @param filename specifies the file
     * @return the number of blocks of that file
     */
    @Override
    public int length(String filename) {
       File file = new File(this.dbDirectory, filename);
       if(!file.exists()) {
           return 0;
       }else{
           fileLocks.putIfAbsent(filename, new Object());
           Object lock = this.fileLocks.get(filename);
           synchronized(lock){
               int length = (int)file.length();
               if(length < this.blocksize){
                   return 0;
               }else{
                   return length / this.blocksize;
               }
           }
       }
    }

    /** Returns the value of the blockSize supplied to the constructor (and
     * conceptually a constant value used by all layers of the PetiteDB system).
     *
     * @return the block size used by the file module
     */
    @Override
    public int blockSize() {
        return this.blocksize;
    }

    private class LRUCache{
        private final int capacity;
        private final Map<File, RandomAccessFile> cacheMap;
        private List<File> cacheList;

        public LRUCache(int capacity) {
            this.capacity = capacity;
            this.cacheMap = new HashMap<>();
            this.cacheList = new ArrayList<>();
        }

        public RandomAccessFile get(File filename) {
            if(cacheMap.containsKey(filename)) {
                RandomAccessFile raf = cacheMap.get(filename);
                cacheList.remove(filename);
                cacheList.add(filename);
                return raf;
            }else{
                if(cacheList.size() > this.capacity) {
                    File one = cacheList.remove(0);
                    RandomAccessFile raf = cacheMap.get(one);
                    try {
                        raf.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    cacheMap.remove(one);
                }
                RandomAccessFile returnRaf;
                try {
                    returnRaf = new RandomAccessFile(filename, "rws");
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
                cacheMap.put(filename, returnRaf);
                cacheList.add(filename);
                return returnRaf;
            }
        }
        public void closeAll() {
            for (RandomAccessFile raf : cacheMap.values()) {
                try {
                    raf.close();
                } catch (IOException e) {
                    // Log the error but continue closing other files
                    System.err.println("Error closing file: " + e.getMessage());
                }
            }
            cacheMap.clear();
            cacheList.clear();
        }

    }

}
