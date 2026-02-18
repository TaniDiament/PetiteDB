package edu.yu.dbimpl.tx.concurrency;

import edu.yu.dbimpl.file.BlockIdBase;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockTable {
    private static ConcurrentHashMap<BlockIdBase, LockInfo> locks = null;
    private long maxWaitTimeInMillis;
    public static final LockTable INSTANCE = new LockTable();
    private LockTable() {
        locks = new ConcurrentHashMap<>();
    }

    public void resetAllLockState() {
        locks.clear();
    }

    public void setMaxWaitTimeInMillis(long maxWaitTimeInMillis) {
        this.maxWaitTimeInMillis = maxWaitTimeInMillis;
    }


    public void sLock(BlockIdBase block, int txNum) throws InterruptedException {
        LockInfo lockInfo = locks.computeIfAbsent(block, k -> new LockInfo());
        long timeout = System.currentTimeMillis() + maxWaitTimeInMillis;
        lockInfo.lock.lock();
        try {
            LockRequest request = new LockRequest(LockType.SHARED, txNum);
            lockInfo.queue.add(request);

            while (lockInfo.xLocks > 0 || lockInfo.queue.peek() != request) {
                if(System.currentTimeMillis() > timeout) {
                    lockInfo.queue.remove(request);
                    throw new LockAbortException("Timeout waiting for sLock on block " + block.number() + " on file " + block.fileName());
                }
                if(!lockInfo.condition.await(timeout-System.currentTimeMillis(), TimeUnit.MILLISECONDS)){
                    lockInfo.queue.remove(request);
                    throw new LockAbortException("Timeout waiting for sLock on block " + block.number() + " on file " + block.fileName());
                }
            }
            lockInfo.queue.remove(request);
            lockInfo.sLocks++;
            lockInfo.sLockHolders.add(txNum);
        } finally {
            lockInfo.lock.unlock();
        }
    }

    public void xLock(BlockIdBase block, int txNum) throws InterruptedException {
        LockInfo lockInfo = locks.computeIfAbsent(block, k -> new LockInfo());
        long timeout = System.currentTimeMillis() + maxWaitTimeInMillis;
        lockInfo.lock.lock();
        try {
            // If I am the only reader and no writer exists, upgrade immediately.
            if (lockInfo.sLockHolders.contains(txNum) && lockInfo.sLocks == 1 && lockInfo.xLocks == 0) {
                lockInfo.sLocks--;
                lockInfo.sLockHolders.remove(txNum);
                lockInfo.xLocks++;
                lockInfo.xLockHolder = txNum;
                return;
            }
            LockRequest request = new LockRequest(LockType.EXCLUSIVE, txNum);
            lockInfo.queue.add(request);
            try {
                while (true) {
                    // Recalculate state
                    boolean isUpgrade = lockInfo.sLockHolders.contains(txNum);
                    int effectiveSLocks = isUpgrade ? lockInfo.sLocks - 1 : lockInfo.sLocks;
                    boolean shouldWait = effectiveSLocks > 0 || lockInfo.xLocks > 0;

                    // --- CUSTOM QUEUE LOGIC STARTS HERE ---
                    if (!shouldWait) {
                        if (isUpgrade) {
                            // RULE: Upgraders yield to SHARED, but jump EXCLUSIVE
                            for (LockRequest r : lockInfo.queue) {
                                if (r == request) break; // Reached myself, path is clear
                                if (r.type == LockType.SHARED) {
                                    shouldWait = true; // Found a Reader ahead, must wait
                                    break;
                                }
                                // If it is EXCLUSIVE, we do nothing (we implicitly jump it)
                            }
                        } else {
                            // Normal Writer: Must be at the very front
                            if (lockInfo.queue.peek() != request) {
                                shouldWait = true;
                            }
                        }
                    }

                    if (!shouldWait) {
                        break; // Safe to acquire lock
                    }

                    // Timeout handling
                    if (System.currentTimeMillis() > timeout) {
                        lockInfo.queue.remove(request);
                        throw new LockAbortException("Timeout waiting for xLock on block " + block.number() + " on file " + block.fileName());
                    }
                    if (!lockInfo.condition.await(timeout - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
                        lockInfo.queue.remove(request);
                        throw new LockAbortException("Timeout waiting for xLock on block " + block.number() + " on file " + block.fileName());
                    }
                }
            } catch (InterruptedException e) {
                lockInfo.queue.remove(request);
                throw e;
            }

            lockInfo.queue.remove(request);

            if (lockInfo.sLockHolders.contains(txNum)) {
                lockInfo.sLocks--;
                lockInfo.sLockHolders.remove(txNum);
            }

            lockInfo.xLocks++;
            lockInfo.xLockHolder = txNum;
        } finally {
            lockInfo.lock.unlock();
        }
    }


    public void unlock(List<BlockIdBase> blocks, int txNum) {
        for (BlockIdBase block : blocks) {
            LockInfo lockInfo = locks.get(block);
            if (lockInfo == null) {
                continue;
            }

            lockInfo.lock.lock();
            try {
                if (Objects.equals(lockInfo.xLockHolder, txNum)) {
                    lockInfo.xLocks--;
                    lockInfo.xLockHolder = null;
                } else if (lockInfo.sLockHolders.contains(txNum)) {
                    lockInfo.sLocks--;
                    lockInfo.sLockHolders.remove(txNum);
                }
                lockInfo.condition.signalAll();
            } finally {
                lockInfo.lock.unlock();
            }
        }
    }

    private enum LockType {
        SHARED, EXCLUSIVE
    }

    private static class LockRequest {
        final LockType type;
        final int txNum;

        LockRequest(LockType type, int txNum) {
            this.type = type;
            this.txNum = txNum;
        }
    }

    private static class LockInfo {
        int sLocks = 0;
        int xLocks = 0;
        final Set<Integer> sLockHolders = new HashSet<>();
        Integer xLockHolder = null;
        final Queue<LockRequest> queue = new LinkedList<>();
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();
    }
}
