package com.experoinc.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.Range;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    private volatile Transaction tx;

    private final Database db;

    private List<Insert> inserts = new LinkedList<>();
    private List<byte[]> deletions = new LinkedList<>();

    private int maxRuns = 1;

    public enum IsolationLevel { SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE }

    private final IsolationLevel isolationLevel;

    private AtomicInteger txCtr = new AtomicInteger(0);

    public FoundationDBTx(Database db, Transaction t, BaseTransactionConfig config, IsolationLevel isolationLevel) {
        super(config);
        tx = t;
        this.db = db;
        this.isolationLevel = isolationLevel;

        switch (isolationLevel) {
            case SERIALIZABLE:
                // no retries
                break;
            case READ_COMMITTED_NO_WRITE:
            case READ_COMMITTED_WITH_WRITE:
                maxRuns = 3;
        }
    }

    public synchronized void restart() {
        txCtr.incrementAndGet();
        if (tx == null) return;
        try {
            tx.cancel();
        } catch (IllegalStateException e) {
            //
        } finally {
            tx.close();
        }
        tx = db.createTransaction();
        // Reapply mutations but do not clear them out just in case this transaction also
        // times out and they need to be reapplied.
        //
        // @todo Note that at this point, the large transaction case (tx exceeds 10,000,000 bytes) is not handled.
        inserts.forEach(insert -> tx.set(insert.getKey(), insert.getValue()));
        deletions.forEach(delete -> tx.clear(delete));
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));
        try {
            tx.cancel();
            tx.close();
            tx = null;
        } catch (Exception e) {
            log.error("failed to rollback", e);
            throw new PermanentBackendException(e);
        } finally {
            if (tx != null)
                tx.close();
        }
    }

    @Override
    public synchronized void commit() throws BackendException {
        boolean failing = true;
        for (int i = 0; i < maxRuns; i++) {
            super.commit();
            if (tx == null) return;
            if (log.isTraceEnabled())
                log.trace("{} committed", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));

            try {
                if (!inserts.isEmpty() || !deletions.isEmpty()) {
                    tx.commit().get();
                } else {
                    // nothing to commit so skip it
                    tx.cancel();
                }
                tx.close();
                tx = null;
                failing = false;
                break;
            } catch (IllegalStateException | ExecutionException e) {
                log.warn("failed to commit transaction", e);
                if (isolationLevel.equals(IsolationLevel.SERIALIZABLE) ||
                        isolationLevel.equals(IsolationLevel.READ_COMMITTED_NO_WRITE)) {
                    break;
                }
                restart();
            } catch (Exception e) {
                log.error("failed to commit", e);
                throw new PermanentBackendException(e);
            }
        }
        if (failing) {
            throw new PermanentBackendException("Max transaction reset count exceeded");
        }
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }

    public byte[] get(final byte[] key) throws PermanentBackendException {
        boolean failing = true;
        byte[] value = null;
        for (int i = 0; i < maxRuns; i++) {
            try {
                ReadTransaction transaction  = getTransaction(this.isolationLevel, this.tx);
                value = transaction.get(key).join();
                failing = false;
                break;
            } catch (Exception e) {
                log.error("failed to get key {}", key, e);
                throw new PermanentBackendException(e);
            }
        }
        if (failing) {
            throw new PermanentBackendException("Max transaction reset count exceeded");
        }
        return value;
    }

    public List<KeyValue> getRange(final byte[] startKey, final byte[] endKey,
                                   final int limit) throws PermanentBackendException {
        boolean failing = true;
        List<KeyValue> result = Collections.emptyList();
        for (int i = 0; i < maxRuns; i++) {
            try {
                ReadTransaction transaction = getTransaction(isolationLevel, this.tx);
                result = transaction.getRange(new Range(startKey, endKey), limit).asList().join();
                if (result == null) return Collections.emptyList();
                failing = false;
                break;
            } catch (Exception e) {
                if(i==maxRuns) {
                    throw new PermanentBackendException("Max transaction reset count for getRange exceeded but last failed with ", e);
                }
                log.warn("raising backend exception for startKey {} endKey {} limit", startKey, endKey, limit, e);
            }
        }
        if (failing) {
            throw new PermanentBackendException("Max transaction reset count exceeded for getRange");
        }
        return result;
    }

    private <T> T getTransaction(IsolationLevel isolationLevel, Transaction tx) {
        if(IsolationLevel.READ_COMMITTED_NO_WRITE.equals(isolationLevel)
                || IsolationLevel.READ_COMMITTED_WITH_WRITE.equals(isolationLevel)) {
            return (T)tx.snapshot();
        } else {
            return (T)tx;
        }
    }

    public synchronized  Map<KVQuery, List<KeyValue>> getMultiRange(final List<Object[]> queries)
            throws PermanentBackendException {
        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();

        try {
            for (int i = 0; i < maxRuns; i++) {
                final List<CompletableFuture> futures = new LinkedList<>();

                for (Object[] obj : queries) {
                    final KVQuery query = (KVQuery) obj[0];
                    final byte[] start = (byte[]) obj[1];
                    final byte[] end = (byte[]) obj[2];

                    if(!resultMap.containsKey(query)) {
                        futures.add(tx.getRange(start, end, query.getLimit()).asList()
                                .whenComplete((res, th) -> {
                                    if (th == null) {
                                        if (res == null) {
                                            res = Collections.emptyList();
                                        }

                                        resultMap.put(query, res);
                                    } else {
                                        log.warn("failed to complete getRange of multiRange", th);
                                    }
                                }));
                    }
                }

                futures.forEach(CompletableFuture::join);
            }

            if(resultMap.size() != queries.size()) {
                throw new PermanentBackendException("Max transaction reset count exceeded for getMultiRange");
            }
        } catch (Exception e) {
            throw new PermanentBackendException("failed to join results", e);
        }

        return resultMap;
    }

    public void set(final byte[] key, final byte[] value) {
        inserts.add(new Insert(key, value));
        tx.set(key, value);
    }

    public void clear(final byte[] key) {
        deletions.add(key);
        tx.clear(key);
    }


    private class Insert {
        private byte[] key;
        private byte[] value;

        public Insert(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() { return this.key; }

        public byte[] getValue() { return this.value; }
    }
}
