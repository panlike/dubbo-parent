package com.dubbo.single.service;

import com.dubbo.single.mapper.OptRecordMapper;
import com.dubbo.single.mapper.UserBasicMapper;
import com.dubbo.single.model.OptRecord;
import com.dubbo.single.model.UserBasic;
import com.dubbo.single.model.UserExtra;
import com.dubbo.single.utils.KafkaLogUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * Created by jishu on 2017/7/10.
 */
@Service("syncUserService")
public class SyncUserService {
    @Resource
    UserBasicMapper userBasicMapper;
    @Resource
    OptRecordMapper optRecordMapper;

    /**
     * 增加用户同步服务，将火图传来的json实例化成java对象传入，数据库操作逻辑与userBasicService中的增加用户逻辑相同，只是在增加用户后需要根据数据库操作的返回结果写入成功或失败日志，并提交kafka offset.
     * 将上述过程纳入事务管理，保证一致性
     * @param userBasic
     * @param userExtra
     * @param topic
     * @param messageId
     * @param message
     * @param con
     * @return
     * @throws IOException
     */
    @Transactional
    public Integer addUser(UserBasic userBasic, UserExtra userExtra, String topic, String messageId, String message, KafkaConsumer<String, String> con) throws IOException {
        Integer result1 = 0;
        Integer result2 = 0;
        result1 = userBasicMapper.addUser(userBasic, userExtra);
        OptRecord record = null;
        if (result1 <= 0){
            return 0;
        }
        record = new OptRecord();
        record.setOptType(OptRecord.OPT_TYPE_REGISTER);
        record.setUserId(userBasic.getId());
        result2 = optRecordMapper.insert(record);
        if (result2 > 0) {
            KafkaLogUtil.log(KafkaLogUtil.LOG_FOR_RECEIVE_SUCCESS, topic, messageId, message);
        } else {
            KafkaLogUtil.log(KafkaLogUtil.LOG_FOR_RECEIVE_ERROR, topic, messageId, message,"数据库操作失败");
        }
        con.commitSync();
        return result2;
    }


}
