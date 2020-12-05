package com.bigdata.flink.sources;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;

/**
 * @ author spencer
 * @ date 2020/6/4 15:21
 * 使用OperatorState保存offset，实现因为异常程序重启时重新消费数据ExactlyOnce
 * 保证 Exactly-Once（已读数据在任务异常重启之后不会再次去读）
 */
public class MyParaExactlyOnceFileSource extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

    private String path;

    private boolean flag = true;

    private long offset = 0;   //偏移量默认值

    private transient ListState<Long> offsetState; //状态数据不参与序列化

    public MyParaExactlyOnceFileSource() { }

    /**
     *
     * @param path /var/data/
     */
    public MyParaExactlyOnceFileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        //获取offset的历史值进行更新
        Iterator<Long> iterator = offsetState.get().iterator();
        while (iterator.hasNext()){
            offset = iterator.next();
        }

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // /var/data/0.txt
        // 获取需要读取的文件以及权限
        RandomAccessFile randomAccessFile = new RandomAccessFile(path + subtaskIndex + ".txt", "r");

        //从文件指定的位置读取数据
        randomAccessFile.seek(offset);

        //多并行线程不安全问题。需要加锁。
        final Object checkpointLock = ctx.getCheckpointLock();

        while (flag){
            String line = randomAccessFile.readLine();
            if (line != null){
                //RandomAccessFile默认使用ISO-8859-1编码读取数据，如果有中文，需要转换编码
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");

                //在更新和发送的时候锁住，以免多线程出错
                synchronized (checkpointLock) {
                    //获取randomAccessFile已经读完数据的指针
                    offset = randomAccessFile.getFilePointer();

                    //将数据发送出去
                    ctx.collect(Tuple2.of(subtaskIndex + "", line));
                }
            } else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {

        flag = false;
    }

    /**
     * 定期将指定的数据状态保存到stateBackend中：生成快照snapshot
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        //将历史值清除
        offsetState.clear();

        //更新最新的状态值
        //offsetState.update(Collections.singletonList(offset));
        offsetState.add(offset);
    }

    /**
     * 初始化OperatorState，生命周期方法，相当于open方法
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        //定义一个状态描述器
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                "offset-state",
                Types.LONG
        );

        //初始化状态
        offsetState = context.getOperatorStateStore().getListState(descriptor);
    }
}
