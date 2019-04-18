//
//import com.shiji.sdp.bi.datax.parse.ColumnEntity;
//import org.apache.kudu.client.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//import java.util.List;
//
//@Component
//public class KuduDao {
//
//    private static final Logger log = LoggerFactory.getLogger(KuduDao.class);
//
//    @Value("${kudu.master-addresses}")
//    private String masterAddresses;
//    @Value("${kudu.default-operation-timeout-ms}")
//    private Integer defaultOperationTimeoutMs;
//    @Value("${kudu.default-socket-read-timeout-ms}")
//    private Integer defaultSocketReadTimeoutMs;
//    @Value("${kudu.default-admin-operation-timeout-ms}")
//    private Integer defaultAdminOperationTimeoutMs;
//    @Value("${kudu.flush-interval}")
//    private Integer flushInterval;
//    @Value("${kudu.mutation-buffer-space}")
//    private Integer mutationBufferSpace;
//
//    KuduClient kuduClient = null;
//
//    // Cache all table on Kudu
//    // private static final Map<String, KuduTable> tableList = new
//    // ConcurrentHashMap<String, KuduTable>();
//
//    @PostConstruct
//    private void init() {
//        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
//                .defaultOperationTimeoutMs(defaultOperationTimeoutMs)
//                .defaultSocketReadTimeoutMs(defaultSocketReadTimeoutMs)
//                .defaultAdminOperationTimeoutMs(defaultAdminOperationTimeoutMs).build();
//    }
//
//    public boolean insert(String dbName, String tableName, List<List<ColumnEntity>> columnRecords) {
//        boolean flag = true;
//        if (columnRecords == null || columnRecords.size() == 0) {
//            throw new IllegalArgumentException("insert method parameter is empty");
//        }
//        String kuduTableName = "presto::" + dbName + "." + tableName;
//        KuduTable table = getTable(kuduTableName);
//        KuduSession sess = newAsyncSession();
//        for (List<ColumnEntity> columns : columnRecords) {
//            try {
//                Upsert upsert = table.newUpsert();
//                PartialRow row = upsert.getRow();
//                for (ColumnEntity entity : columns) {
//                    row.addString(entity.getColumnName(), entity.getColumnValue());
//                }
//                sess.apply(upsert);
//            } catch (KuduException e) {
//                log.error("Kudu exception while insert data.", e);
//                flag = false;
//            }
//        }
//        try {
//            sess.flush();
//        } catch (KuduException e) {
//            log.error("Kudu exception while insert data.", e);
//            flag = false;
//        } finally {
//            try {
//                sess.close();
//            } catch (KuduException e) {
//                log.error("Kudu session close.", e);
//            }
//        }
//        return flag;
//    }
//
//    /****
//     * table cache
//     *
//     * @param tableName
//     * @return
//     */
//    private KuduTable getTable(String tableName) {
//        KuduTable table = null;
//        try {
//            table = kuduClient.openTable(tableName);
//        } catch (KuduException e) {
//            throw new KuduOperationException("Kudu client open table \"" + tableName + "\" fail. ", e);
//        }
//        return table;
//    }
//
//    /****
//     * FlushMode:AUTO_FLUSH_BACKGROUND
//     *
//     * @return
//     */
//    private KuduSession newAsyncSession() {
//        KuduSession session = kuduClient.newSession();
//        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
//        session.setFlushInterval(flushInterval);
//        session.setMutationBufferSpace(mutationBufferSpace);
//        return session;
//    }
//}