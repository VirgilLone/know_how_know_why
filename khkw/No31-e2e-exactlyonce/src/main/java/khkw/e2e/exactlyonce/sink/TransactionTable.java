package khkw.e2e.exactlyonce.sink;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.sink
 * 功能描述: 这是e2e Exactly-once 的临时表抽象。 掌控一个事务所有的信息
 * 作者： 孙金城
 * 日期： 2020/7/16
 */
public class TransactionTable implements Serializable {
    private transient TransactionDB db; // todo 本次事务刚要结束，还未完成的时候，会生成下一次cp的事务信息
    private final String transactionId;
    private final List<Tuple3<String, Long, String>> buffer = new ArrayList<>();    // todo 本次事务的所有信息

    public TransactionTable(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public TransactionTable insert(Tuple3<String, Long, String> value) {
        initDB();
        // 投产的话，应该逻辑应该写到远端DB或者文件系统等。
        buffer.add(value);
        return this;
    }

    public TransactionTable flush() {
        initDB();
        db.firstPhase(transactionId, buffer);
        return this;
    }

    public void close() {
        buffer.clear();
    }

    private void initDB() {
        if (null == db) {
            db = TransactionDB.getInstance();
        }
    }
}
