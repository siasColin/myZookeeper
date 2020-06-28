package cn.colin.zookeeper;

/**
 * @Package: cn.colin.zookeeper
 * @Author: sxf
 * @Date: 2020-6-26
 * @Description: 多线程售票任务类
 */
public class SellTicktDemo implements  Runnable {
    //定义票的总数
    private int total = 100;

    //定义票的编号
    private int no = total+1;


    @Override
    public void run() {
        MyLock lock = null;
        while(true){
            try{
                lock = new MyLock();
                //获取锁
                lock.acquireLock();
                if(this.total > 0){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String msg = Thread.currentThread().getName()+" 售出第   "+(this.no -this.total) +"  张票";
                    System.out.println(msg);
                    this.total--;
                }else{
                    System.out.println("票已售完，请下次再来！");
                    System.exit(0);
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                try {
                    if(lock != null){
                        //释放锁
                        lock.releaseLock();
                        lock = null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
