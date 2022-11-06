package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import simpledb.common.Permissions;
import simpledb.storage.HeapPageId;
import simpledb.storage.PageId;
import simpledb.storage.PageLock;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.transaction.TransactionId;

/** Unit test for PageLock. */
public class PageLockTest extends SimpleDbTestBase {

  /** Time to wait before checking the state of lock contention, in ms */
  private static final int TIMEOUT = 100;

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Generic unit test structure to grab a lock in a new thread.
   *
   * @param tid      the transaction Id
   * @param lock     the lock to acquire
   * @param perm     the type of lock desired
   * @param expected the expected result
   */
  public void grabLock(TransactionId tid, PageLock lock, Permissions perm, boolean expected) throws Exception {
    Thread t;
    if (perm == Permissions.READ_ONLY) {
      t = new Thread() {
        public void run() {
          lock.sLock(tid);
        }
      };
    } else {
      t = new Thread() {
        public void run() {
          lock.xLock(tid);
        }
      };
    }
    t.start();

    Thread.sleep(TIMEOUT);
    if (perm == Permissions.READ_ONLY) {
      assertEquals(expected, lock.holdsSLock(tid));
    } else {
      assertEquals(expected, lock.holdsXLock(tid));
    }
  }

  @Test
  public void multipleSLocks() throws Exception {
    PageId pid = new HeapPageId(0, 0);
    PageLock lock = new PageLock(pid);
    for (int i = 0; i < 3; i++) {
      TransactionId t = new TransactionId();
      grabLock(t, lock, Permissions.READ_ONLY, true);
    }
  }

  @Test
  public void multipleXLocks() throws Exception {
    PageId pid = new HeapPageId(0, 0);
    PageLock lock = new PageLock(pid);
    TransactionId t0 = new TransactionId();
    grabLock(t0, lock, Permissions.READ_WRITE, true);

    for (int i = 0; i < 3; i++) {
      TransactionId t = new TransactionId();
      grabLock(t, lock, Permissions.READ_ONLY, false);
    }
    for (int i = 0; i < 3; i++) {
      TransactionId t = new TransactionId();
      grabLock(t, lock, Permissions.READ_WRITE, false);
    }
  }

  @Test
  public void release() throws Exception {
    PageId pid = new HeapPageId(0, 0);
    PageLock lock = new PageLock(pid);
    TransactionId t0 = new TransactionId();
    lock.xLock(t0);
    assertTrue(lock.holdsXLock(t0));
    lock.xUnlock(t0);
    assertFalse(lock.holdsXLock(t0));
    lock.sLock(t0);
    assertTrue(lock.holdsSLock(t0));
    lock.sUnlock(t0);
    assertFalse(lock.holdsSLock(t0));
  }
}
