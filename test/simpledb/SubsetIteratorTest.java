package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Predicate;
import simpledb.execution.SubsetIterator;
import simpledb.optimizer.TableStats;
import simpledb.storage.Field;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;
import simpledb.transaction.TransactionAbortedException;

public class SubsetIteratorTest extends SimpleDbTestBase {
	
	List<Integer> list;

	@Before public void setUp() throws Exception {
		super.setUp();
	}
	
	/**
	 * Verify the functionality of our SubsetIterator.
	 */
	@SuppressWarnings("unchecked")
	@Test public void subsetIteratorTest() {
		list = Arrays.asList(1, 2, 3, 4, 5, 6);
		SubsetIterator<Integer> iter;
		Set<Set<Integer>> res;

		iter = new SubsetIterator<>(list, 0);
		res = new HashSet<>();
		while (iter.hasNext()) {
			Set<Integer> next = iter.next();
			Assert.assertEquals(0, next.size());
			res.add(next);
		}
		Assert.assertEquals(1, res.size());

		iter = new SubsetIterator<>(list, 1);
		res = new HashSet<>();
		while (iter.hasNext()) {
			Set<Integer> next = iter.next();
			Assert.assertEquals(1, next.size());
			res.add(next);
		}
		Assert.assertEquals(6, res.size());
		
		iter = new SubsetIterator<>(list, 4);
		res = new HashSet<>();
		while (iter.hasNext()) {
			Set<Integer> next = iter.next();
			Assert.assertEquals(4, next.size());
			res.add(next);
		}
		Assert.assertEquals(15, res.size());

		iter = new SubsetIterator<>(list, 6);
		res = new HashSet<>();
		while (iter.hasNext()) {
			Set<Integer> next = iter.next();
			Assert.assertEquals(6, next.size());
			res.add(next);
		}
		Assert.assertEquals(1, res.size());
	}

}
