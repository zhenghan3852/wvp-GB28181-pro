package com.genersoft.iot.vmp.conf;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.io.File;

/**
 * Web MVC 配置
 * 用于配置静态资源映射
 */
@Configuration
@Slf4j
public class WebMvcConfig implements WebMvcConfigurer {

    @Value("${image.path:img}")  // 从配置文件读取图片路径，默认值为 img
    private String imagePath;

    /**
     * 配置静态资源映射
     * 将 /img/** 映射到配置的图片文件夹
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // 获取图片目录
        File imgDir = new File(imagePath);
        
        // 如果是相对路径，转换为绝对路径
        if (!imgDir.isAbsolute()) {
            imgDir = new File(System.getProperty("user.dir"), imagePath);
        }
        
        log.info("[WebMvcConfig] 配置静态资源映射");
        log.info("[WebMvcConfig] 项目根目录: {}", System.getProperty("user.dir"));
        log.info("[WebMvcConfig] 图片目录: {}", imgDir.getAbsolutePath());
        
        // 配置 /img/** 映射到图片文件夹
        registry.addResourceHandler("/img/**")
                .addResourceLocations("file:" + imgDir.getAbsolutePath() + "/")
                .setCachePeriod(3600)  // 缓存 1 小时
                .resourceChain(true);
        
        log.info("[WebMvcConfig] 静态资源映射配置完成");
    }
}
