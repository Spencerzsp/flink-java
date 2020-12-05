package com.bigdata.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.List;

/**
 * 对于多并行的source，使用OperatorState保存状态：记录每行内容所对应的文件
 * 3> (2,spark flink)
   2> (1,spark flink)
   1> (0,hello spark)
 * @ description:
 * @ author: spencer
 * @ date: 2020/11/27 16:48
 */
public class OperatorStateDemo2 extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

    private transient ListState<Long> offsetState;
    private Long offset = 0L;
    private boolean flag = true;
    private String path;

    public OperatorStateDemo2() {
    }

    public OperatorStateDemo2(String path) {
        this.path = path;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        offsetState.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<Long>(
                "list-state-descripter",
                Types.LONG
        );
        // getRuntimeContext().getState():拿到的都是KeyedState
        // 只能使用context.getOperatorStateStore()获取
        offsetState = context.getOperatorStateStore().getListState(listStateDescriptor);
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        // 遍历历史值，准备开始更新
        Iterator<Long> iterator = offsetState.get().iterator();
        while (iterator.hasNext()){
            offset = iterator.next();
        }
        int index = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile random = new RandomAccessFile(path + index + ".txt", "r");

        // 从指定文件的offset读取数据
        random.seek(offset);
        while (flag){
            final Object lock = ctx.getCheckpointLock();
            String line = random.readLine();
            if (line != null){
                //RandomAccessFile默认使用ISO-8859-1编码读取数据，如果有中文，需要转换编码
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                synchronized (lock){
                    offset = random.getFilePointer();
                    ctx.collect(Tuple2.of(index + "", line));
                }
            } else {
                Thread.sleep(2000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

//        // 自定义数据源
//        DataStreamSource<Integer> streamSource = env.addSource(new SourceFunction<Integer>() {
//            private boolean flag = true;
//            int i = 0;
//
//            @Override
//            public void run(SourceContext<Integer> ctx) throws Exception {
//                while (i <= 1000) {
//
//                    ctx.collect(i++);
//
////                    Thread.sleep(2000);
//                }
//
//            }
//
//            @Override
//            public void cancel() {
//                flag = false;
//            }
//        });
//        streamSource.print();

        OperatorStateDemo2 source = new OperatorStateDemo2("D:/IdeaProjects/flink-java/src/main/resources/data/");
        DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(source);

        dataStreamSource.print();

        env.execute();
    }
}
