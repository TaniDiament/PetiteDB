package edu.yu.dbimpl.buffer;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.log.LogMgrBase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Specifies the public API for the BufferMgr implementation by requiring all
 * BufferMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A BufferMgr manages a constrained set of main-memory buffer pages by pinning
 * and unpinning the association of buffers to specific disk blocks.  Unless
 * explictly directed via a client invocation of flushAll(), movement from
 * main-memory to disk is (at best) translucent to clients.  Implementations
 * are REQUIRED to ONLY move from main-memory to disk if necessary (as defined
 * in lecture).
 *
 * The DBMS has exactly one BufferMgr instance (singleton pattern), which is
 * created during system startup.
 *
 * Design note: it is recommended, but not required, for the buffer manager to
 * delegate all read/write function of its files to the file manager.
 *
 * @author Avraham Leff
 */
public class BufferMgr extends BufferMgrBase {
    private final EvictionPolicy evictionPolicy;
    private final LogMgrBase logMgr;
    private final FileMgrBase fileMgr;
    private final int nBuffers;
    private final int maxWaitTime;
    private BufferBase[] bufferArray;
    private ConcurrentMap<BufferBase, Integer> pinMap;
    private ConcurrentMap<BlockIdBase, BufferBase> blockMap;
    private final ReentrantLock bufferQueueLock = new ReentrantLock(true);
    private final Condition bufferAvailable = bufferQueueLock.newCondition();
    private final Object lock = new Object();
    private int current = 0;

    /** Creates a buffer manager having the specified number of buffer slots, and
     * use the EvictionPolicy.NAIVE.
     *
     * The buffer manager MUST access the DBConfiguration singleton to determine
     * if implementation specific actions must be taken to (re)initialize the
     * necessary state.  The client is responsible for creating the file and log
     * managers before invoking the buffer manager constructor.  The buffer
     * manager may therefore assume that these module managers have been
     * correctly initially when this constructor is invoked.
     *
     * @param fileMgr file manager singleton
     * @param logMgr  log manager singleton
     * @param nBuffers number of buffers to allocate in main-memory
     * @param maxWaitTime maximum number of milliseconds to wait before throwing
     * a BufferAbortException to a client invoking pin().  Must be greater than 0.
     * @see #pin
     */
    public BufferMgr(FileMgrBase fileMgr, LogMgrBase logMgr, int nBuffers, int maxWaitTime) {
        super(fileMgr, logMgr, nBuffers, maxWaitTime);
        if(maxWaitTime <= 0 || fileMgr == null || logMgr == null || nBuffers <= 0){
            throw new IllegalArgumentException("invalid parameters");
        }
        evictionPolicy = EvictionPolicy.NAIVE;
        this.logMgr = logMgr;
        this.fileMgr = fileMgr;
        this.nBuffers = nBuffers;
        this.maxWaitTime = maxWaitTime;
        this.initializeBufferManager();
    }

    /** Creates a buffer manager having the specified number of buffer slots, and
     * specify the EvictionPolicy that must be used.
     *
     * Be sure to also see the Javadoc on the default eviction policy
     * constructor.
     *
     * @param fileMgr file manager singleton
     * @param logMgr  log manager singleton
     * @param nBuffers number of buffers to allocate in main-memory
     * @param maxWaitTime maximum number of milliseconds to wait before throwing
     * a BufferAbortException to a client invoking pin().  Must be greater than 0.
     * @param policy the eviction policy to be used
     * @see #pin
     * @see #EvictionPolicy
     */
    public BufferMgr(FileMgrBase fileMgr, LogMgrBase logMgr, int nBuffers, int maxWaitTime, EvictionPolicy policy) {
        super(fileMgr, logMgr, nBuffers, maxWaitTime, policy);
        if(maxWaitTime <= 0 || fileMgr == null || logMgr == null || nBuffers <= 0 || policy == null){
            throw new IllegalArgumentException("invalid parameters");
        }
        this.evictionPolicy = policy;
        this.logMgr = logMgr;
        this.fileMgr = fileMgr;
        this.nBuffers = nBuffers;
        this.maxWaitTime = maxWaitTime;
        this.initializeBufferManager();
    }

    private void initializeBufferManager() {
        this.pinMap = new ConcurrentHashMap<>();
        this.bufferArray = new BufferBase[nBuffers];
        this.blockMap = new ConcurrentHashMap<>();
        for (int i = 0; i < nBuffers; i++) {
            Buffer buffer = new Buffer(fileMgr, logMgr);
            pinMap.put(buffer, 0);
            bufferArray[i] = buffer;
        }
    }

    /** Returns the number of available (i.e. unpinned) buffers.
     *
     * @return the number of available buffers
     */
    @Override
    public int available() {
        int count = 0;
        synchronized(lock) {
            List<Integer> pinCounts = new ArrayList<>(pinMap.values());
            for (Integer pinCount : pinCounts) {
                if (pinCount == 0) {
                    count++;
                }
            }
            return count;
        }
    }

    /** Flushes all modified ("dirty") buffers modified by the specified
     * transaction.  Any association between the transaction and its buffers is
     * removed.
     *
     * @param txnum the transaction's id number
     * @throws IllegalArgumentException if txnum is negative
     * @see BufferBase.setModified
     */
    @Override
    public void flushAll(int txnum) {
        if(txnum < 0){
            throw new IllegalArgumentException("txnum is negative");
        }
        synchronized (lock){
            for(BufferBase b : bufferArray){
                if(((Buffer)b).isModified() && ((Buffer) b).hasTXN(txnum)){
                    ((Buffer) b).flush();
                    fileMgr.write(b.block(), b.contents());
                }
            }
        }
    }

    /**
     * used for recovery
     * @return
     */
    public void recoverFlush(){
        synchronized (lock){
            for(BufferBase b : bufferArray){
                if(((Buffer)b).isModified()){
                    ((Buffer) b).flush();
                    fileMgr.write(b.block(), b.contents());
                }
            }
        }
    }

    /** Unpins the specified data buffer. If its pin count goes to zero, must
     * inform all clients currently blocked invoking pin() that a buffer instance
     * is now available.
     *
     * @param buffer the buffer to be unpinned
     * @throws IllegalArgumentException if buffer is null.
     * @throws IllegalArgumentException if buffer isn't currently pinned
     * @see #pin
     */
    @Override
    public void unpin(BufferBase buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        if(!pinMap.containsKey(buffer) || pinMap.get(buffer) <= 0) {
            throw new IllegalArgumentException("Buffer is not currently pinned");
        }
        int pn;
        synchronized(lock) {
            pn = pinMap.get(buffer) - 1;
            pinMap.put(buffer, pn);
            ((Buffer)buffer).unpin();
        }
        if(pn == 0){
            bufferQueueLock.lock();
            try {
                bufferAvailable.signal();
            } finally {
                bufferQueueLock.unlock();
            }
        }
    }

    /** Pins a buffer to the specified block, writing the current contents to
     * disk if modified by the previous client and a new disk block is being read
     * from disk.  If no buffers are currently available, the client will block,
     * waiting for a buffer instance to become available.  If no buffer becomes
     * available within the time specified by the "maxWaitTime" value supplied to
     * the constructor, throws a {@link BufferAbortException}.
     *
     * @param blk a reference to a disk block
     * @return the buffer pinned to that block
     * @throws IllegalArgumentException if BlockId is null.
     * @throws BufferAbortException if the client times out waiting for a buffer
     * to become available.
     */
    @Override
    public BufferBase pin(BlockIdBase blk) {
        if (blk == null) {
            throw new IllegalArgumentException("BlockId cannot be null");
        }
        BufferBase buffer = checkIfWeHave(blk);
        if (buffer != null) {
            return buffer;
        }
        BufferBase returnBuffer = null;
        bufferQueueLock.lock();
        int available = available();
        if (available == 0) {
            getInLine();
        }

        int start = (evictionPolicy == EvictionPolicy.NAIVE) ? 0 : current;

        synchronized (lock){
            bufferQueueLock.unlock();
            for(int i = 0; i < nBuffers; i++) {
                int q = (i + start) % nBuffers;
                Buffer ourBuf = (Buffer) bufferArray[q];
                if(!ourBuf.isPinned()){
                    ourBuf.pin();
                    int w = pinMap.get(ourBuf) + 1;
                    pinMap.put(ourBuf, w);
                    if(ourBuf.isModified()){
                        flushAndClear(ourBuf);
                    }else{
                        BlockIdBase block = ourBuf.block();
                        if(block != null){
                            blockMap.remove(block);
                        }
                    }
                    blockMap.put(blk, ourBuf);
                    ourBuf.setBlock(blk);
                    PageBase p = new Page(fileMgr.blockSize());
                    fileMgr.read(blk, p);
                    ourBuf.setPage(p);
                    returnBuffer = ourBuf;
                    current = (current + 1 == nBuffers) ? 0 : ++current;
                    break;
                }
                if(q == nBuffers-1) {
                    start = 0;
                }
            }
        }

        return returnBuffer;
    }

    private void flushAndClear(BufferBase buffer){
        fileMgr.write(buffer.block(), buffer.contents());
        ((Buffer)buffer).flush();
        blockMap.remove(buffer.block());
    }

    /**
     * Queues the calling thread to wait for an open buffer
     */
    private void getInLine(){
        boolean weNext;
        try {
            weNext = bufferAvailable.await(maxWaitTime, TimeUnit.MILLISECONDS);
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            bufferQueueLock.unlock();
            throw new RuntimeException("Interrupted while waiting for buffer");
        }
        if (!weNext) {
            bufferQueueLock.unlock();
            throw new BufferAbortException("Timed out waiting for open buffer");
        }
    }

    /**
     * Checks if we have the block already pinned to a buffer
     * @param blk
     * @return buffer in already in memory
     */
    private BufferBase checkIfWeHave(BlockIdBase blk){
        boolean found = false;
        synchronized(lock) {
            if(blockMap.containsKey(blk)) {
                BufferBase buffer = blockMap.get(blk);
                int pin = pinMap.get(blockMap.get(blk)) + 1;
                pinMap.put(blockMap.get(blk), pin);
                ((Buffer)buffer).pin();
                found = true;
            }
            return found ? blockMap.get(blk) : null;
        }
    }

    /** Returns the EvictionPolicy used by the buffer manager.
     */
    @Override
    public EvictionPolicy getEvictionPolicy() {
        return this.evictionPolicy;
    }
}
