package com.nexr.traffic.flume.common;

import java.util.ArrayList;
import java.util.List;

public class FixedArrayList<E> {

	private static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
	private List<E> list;
	private int maxSize;
	private int idx;
	
	public FixedArrayList() {
		this(DEFAULT_MAX_SIZE);
	}
	
	public FixedArrayList(int maxSize) {
		this.maxSize = maxSize;
		this.list = new ArrayList<E>(maxSize);
		
	}
	
	public int size() {
		return this.maxSize;
	}
	
	public boolean contains(Object o) {
		return this.list.contains(o);
	}
	
	public boolean add(E e) {
		if (this.idx >= this.maxSize) {
			this.idx = 0;
		}
        if (this.idx >= this.list.size()) {
            list.add(e);
        } else {
            list.set(idx, e);
        }
        idx++;
        return true;		
	}
	
    public boolean remove(Object o) {
        return this.list.remove(o);
    }

    public void clear() {
        this.list.clear();
    }
}
