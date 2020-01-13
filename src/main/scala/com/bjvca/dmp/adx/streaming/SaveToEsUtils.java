package com.bjvca.dmp.adx.streaming;

import com.bjvca.dmp.commonutils.ConfUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import java.util.List;
import java.util.logging.Logger;

public class SaveToEsUtils {
    public static void updateFlagToES(ConfUtils confUtil,List<String> listDealidCosted) throws Exception {

        Logger logging = Logger.getLogger("接口Log");
        if (listDealidCosted.isEmpty()) {
            logging.warning("无达标dealid");
            return;
        }

        //es客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(confUtil.adxStreamingEsHost(), 9200, "http"),
                        new HttpHost(confUtil.adxStreamingEsHost(), 9201, "http")));

        //批量处理
        BulkRequest request = new BulkRequest();
        String[] split = "vcaadx/test".split("/");
        String index = split[0];
        String type = split[1];

        //日消达标
        for (int i = 0; i < listDealidCosted.size(); i++) {
            String dealid = listDealidCosted.get(i);
            UpdateRequest updateRequest = new UpdateRequest(index, type, dealid+confUtil.nowTime());
            updateRequest.doc(XContentType.JSON, "stop", 1);
            request.add(updateRequest);
            logging.warning("日限额达标：" + dealid);
        }
        
        //提交
        BulkResponse bulkResponse = client.bulk(request);
        //关闭
        client.close();
    }
}