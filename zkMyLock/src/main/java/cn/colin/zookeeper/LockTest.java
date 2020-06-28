package cn.colin.zookeeper;

/**
 * @Package: cn.colin.zookeeper
 * @Author: sxf
 * @Date: 2020-6-26
 * @Description:
 */
public class LockTest {
    public static void main(String[] args) {
        //得到对象
        SellTicktDemo std = new SellTicktDemo();

        //把对象放入线程中
        Thread t1 = new Thread(std,"售票窗口1");
        Thread t2 = new Thread(std,"售票窗口2");
        Thread t3 = new Thread(std,"售票窗口3");
        Thread t4 = new Thread(std,"售票窗口4");

        t1.start();
        t2.start();
        t3.start();
        t4.start();
    }
}
