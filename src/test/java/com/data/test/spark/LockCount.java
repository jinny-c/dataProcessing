package com.data.test.spark;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockCount {
	private int count;
	private Lock lock;
	private Condition condition;
	private static LockCount instance = null;

	private LockCount() {
		count = 0;
		// 为true表示为公平锁，为fasle为非公平锁。默认情况下，如果使用无参构造器，则是非公平锁
		// lock = new ReentrantLock(true);// 公平锁
		lock = new ReentrantLock();
		// 即便是公平锁，如果通过不带超时时间限制的tryLock()的方式获取锁的话，它也是不公平的
		// 但是带有超时时间限制的tryLock(long timeout, TimeUnit unit)方法则不一样，还是会遵循公平或非公平的原则的
		condition = lock.newCondition();
	}

	public static LockCount getInstance() {
		if (instance == null) {
			instance = new LockCount();
		}
		return instance;
	}

	public int getCount() {
		// TODO Auto-generated method stub
		return method();
	}

	private int method() {
		try {
			lock.lock();
			return count++;
		} catch (Exception e) {
			e.getStackTrace();
		} finally {
			lock.unlock();
		}
		return count;
	}
	
	public static void main(String[] args) throws Exception {
		String s1 = "汉字123";
		String s2 = new String(s1.getBytes(),"GBK");
		System.out.println(new String(s2.getBytes("GBK"),"UTF-8"));
		System.out.println(new String(s1.getBytes(),"UTF-8"));
	}
}
