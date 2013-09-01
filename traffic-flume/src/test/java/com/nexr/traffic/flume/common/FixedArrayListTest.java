package com.nexr.traffic.flume.common;

import org.junit.Assert;
import org.junit.Test;

public class FixedArrayListTest {

	private FixedArrayList<String> fixedArrayList;

	@Test
	public void test() {
		fixedArrayList = new FixedArrayList<String>(2);
		fixedArrayList.add("A");
		fixedArrayList.add("B");

		Assert.assertTrue(fixedArrayList.contains("A"));
		Assert.assertTrue(fixedArrayList.contains("B"));
		
		fixedArrayList.add("C");
		Assert.assertTrue(fixedArrayList.contains("C"));
		Assert.assertTrue(fixedArrayList.contains("B"));
		Assert.assertFalse(fixedArrayList.contains("A"));
		fixedArrayList.add("D");
		Assert.assertTrue(fixedArrayList.contains("D"));
		Assert.assertTrue(fixedArrayList.contains("C"));
		Assert.assertFalse(fixedArrayList.contains("B"));
		Assert.assertFalse(fixedArrayList.contains("A"));
	}

}
