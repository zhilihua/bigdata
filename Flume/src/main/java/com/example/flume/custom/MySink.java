package com.example.flume.custom;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 从配置文件中读取一个后缀，将event的内容读取后，拼接后缀进行输出
public class MySink extends AbstractSink implements Configurable {
    private String suffix;
    private Logger logger= LoggerFactory.getLogger(MySink.class);
    //核心方法：处理sink逻辑
    // Status.ready:  成功传输了一个或多个event
    // Status.backoff:  从channel中无法获取数据
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        //获取当前sink对接channel
        Channel c = getChannel();

        //声明Event，用来接收channel中event
        Event e = null;

        Transaction transaction = c.getTransaction();

        try {
            //获取take事务对象
            //开启事务
            transaction.begin();

            ////如果channel中，没有可用的event，此时e会是null
            e = c.take();

            if (e == null) {
                status = status.BACKOFF;
            } else {
                //取到数据后，执行拼接后缀进行输出
                logger.info(new String(e.getBody()) + suffix);
            }
            //提交事务
            transaction.commit();
        } catch (ChannelException el){
            //回滚事务
            transaction.rollback();
            status = Status.BACKOFF;
            el.printStackTrace();
        }finally {
            //关闭事务对象
            transaction.close();
        }

        return status;
    }

    //从配置中读取配置的参数
    @Override
    public void configure(Context context) {
        suffix=context.getString("suffix", ":hi");
    }
}