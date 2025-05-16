package com.genersoft.iot.vmp.conf;


import com.genersoft.iot.vmp.conf.exception.ControllerException;
import com.genersoft.iot.vmp.media.bean.MediaServer;
import com.genersoft.iot.vmp.media.service.IMediaServerService;
import com.genersoft.iot.vmp.service.bean.CloudRecordItem;
import com.genersoft.iot.vmp.storager.dao.CloudRecordServiceMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 录像文件定时删除
 */
@Slf4j
@Component
public class CloudRecordTimer {

    @Autowired
    private IMediaServerService mediaServerService;

    @Autowired
    private CloudRecordServiceMapper cloudRecordServiceMapper;

    /**
     * 定时清理超过1小时的录像文件
     */
    @Scheduled(cron = "0 0 */1 * * ?")   // 每小时执行一次
    public void execute(){
        log.info("[录像文件定时清理] 开始清理超过1小时的录像文件");
        
        List<MediaServer> mediaServerItemList = mediaServerService.getAllOnline();
        if (mediaServerItemList.isEmpty()) {
            return;
        }
        
        long result = 0;
        for (MediaServer mediaServerItem : mediaServerItemList) {
            // 设置为1小时前
            Calendar lastCalendar = Calendar.getInstance();
            lastCalendar.add(Calendar.HOUR_OF_DAY, -1);
            Long lastDate = lastCalendar.getTimeInMillis();

<<<<<<< HEAD
            // 获取超过1小时的未收藏录像
            List<CloudRecordItem> cloudRecordItemList = cloudRecordServiceMapper.queryRecordListForDelete(lastDate, mediaServerItem.getId());
            if (cloudRecordItemList.isEmpty()) {
                continue;
            }

            // 删除文件
            for (CloudRecordItem cloudRecordItem : cloudRecordItemList) {
                String date = new File(cloudRecordItem.getFilePath()).getParentFile().getName();
                boolean deleteResult = mediaServerService.deleteRecordDirectory(mediaServerItem, cloudRecordItem.getApp(),
                        cloudRecordItem.getStream(), date, cloudRecordItem.getFileName());
                if (deleteResult) {
                    log.info("[录像文件定时清理] 删除超过1小时的文件: {}", cloudRecordItem.getFilePath());
=======
                // 获取到截至日期之前的录像文件列表，文件列表满足未被收藏和保持的。这两个字段目前共能一致，
                // 为我自己业务系统相关的代码，大家使用的时候直接使用收藏（collect）这一个类型即可
                List<CloudRecordItem> cloudRecordItemList = cloudRecordServiceMapper.queryRecordListForDelete(lastDate, mediaServerItem.getId());
                if (cloudRecordItemList.isEmpty()) {
                    continue;
                }
                // TODO 后续可以删除空了的过期日期文件夹
                for (CloudRecordItem cloudRecordItem : cloudRecordItemList) {
                    String date = new File(cloudRecordItem.getFilePath()).getParentFile().getName();
                    try {
                        boolean deleteResult = mediaServerService.deleteRecordDirectory(mediaServerItem, cloudRecordItem.getApp(),
                                cloudRecordItem.getStream(), date, cloudRecordItem.getFileName());
                        if (deleteResult) {
                            log.warn("[录像文件定时清理] 删除磁盘文件成功： {}", cloudRecordItem.getFilePath());
                        }
                    }catch (ControllerException ignored) {}

>>>>>>> c981e7f470a2e8fc85165ef7ebd78b8c55e3377b
                }
            }
            result += cloudRecordServiceMapper.deleteList(cloudRecordItemList);
        }
        
        log.info("[录像文件定时清理] 共清理{}个超过1小时的录像文件", result);
    }
}
