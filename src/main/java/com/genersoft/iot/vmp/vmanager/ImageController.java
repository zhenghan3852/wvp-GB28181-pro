package com.genersoft.iot.vmp.vmanager;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 图片管理接口
 * 用于读取 img 文件夹下的所有图片文件
 */
@Tag(name = "图片管理")
@Slf4j
@RestController
@RequestMapping("/api/image")
public class ImageController {

    @Value("${server.port:18080}")
    private int serverPort;

    @Value("${image.path:img}")
    private String imagePath;

    /**
     * 获取所有图片文件列表
     * @return 图片文件列表
     */
    @GetMapping("/list")
    @ResponseBody
    @Operation(summary = "获取图片列表", security = @SecurityRequirement(name = "Authorization"))
    public Map<String, Object> getImageList() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // 尝试多种方式获取 img 文件夹
            File imgDir = findImageDirectory();
            
            log.info("[图片管理] 读取图片目录: {}", imgDir.getAbsolutePath());
            
            // 检查目录是否存在
            if (!imgDir.exists() || !imgDir.isDirectory()) {
                log.warn("[图片管理] 图片目录不存在: {}", imgDir.getAbsolutePath());
                log.warn("[图片管理] 尝试的路径:");
                log.warn("[图片管理]   1. user.dir: {}", new File(System.getProperty("user.dir"), "img").getAbsolutePath());
                log.warn("[图片管理]   2. JAR 同级: {}", new File(getJarDirectory(), "img").getAbsolutePath());
                result.put("code", 200);
                result.put("message", "图片目录不存在");
                result.put("data", new HashMap<>());
                return result;
            }
            
            // 按文件夹分组获取图片
            Map<String, Map<String, Object>> groupedImages = scanImageFilesByFolder(imgDir);
            
            result.put("code", 200);
            result.put("message", "成功");
            result.put("data", groupedImages);
            
            int totalImages = groupedImages.values().stream()
                    .mapToInt(folder -> ((List<?>) folder.get("data")).size())
                    .sum();
            
            log.info("[图片管理] 成功读取 {} 个文件夹，共 {} 个图片文件", groupedImages.size(), totalImages);
            
        } catch (Exception e) {
            log.error("[图片管理] 读取图片列表失败", e);
            result.put("code", 500);
            result.put("message", "读取图片列表失败: " + e.getMessage());
            result.put("data", new ArrayList<>());
        }
        
        return result;
    }
    
    /**
     * 按文件夹分组扫描图片文件
     * @param imgDir 图片根目录
     * @return 按文件夹分组的图片数据
     */
    private Map<String, Map<String, Object>> scanImageFilesByFolder(File imgDir) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        
        File[] folders = imgDir.listFiles();
        if (folders == null || folders.length == 0) {
            log.info("[图片管理] 图片目录为空");
            return result;
        }
        
        for (File folder : folders) {
            if (folder.isDirectory()) {
                String folderName = folder.getName();
                List<Map<String, Object>> imageList = new ArrayList<>();
                
                log.info("[图片管理] 扫描文件夹: {}", folderName);
                
                // 加载 JSON 文件中的图片信息
                Map<String, Object> jsonImageInfo = loadImageInfoFromJson(folder);
                
                // 扫描文件夹中的图片
                File[] files = folder.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isFile() && isImageFile(file.getName())) {
                            Map<String, Object> imageInfo = new HashMap<>();
                            imageInfo.put("name", file.getName());
                            imageInfo.put("path", folderName + "/" + file.getName());
                            imageInfo.put("size", file.length());
                            imageInfo.put("lastModified", file.lastModified());
                            imageInfo.put("url", "/img/" + folderName + "/" + file.getName());
                            imageInfo.put("fullUrl", "http://localhost:" + serverPort + "/img/" + folderName + "/" + file.getName());
                            
                            // 从 JSON 中获取对应的图片信息
                            if (jsonImageInfo.containsKey(file.getName())) {
                                Map<String, Object> jsonInfo = (Map<String, Object>) jsonImageInfo.get(file.getName());
                                imageInfo.putAll(jsonInfo);
                                log.info("[图片管理] 匹配到 JSON 信息: {}", file.getName());
                            }
                            
                            imageList.add(imageInfo);
                            log.info("[图片管理] 找到图片: {}/{}", folderName, file.getName());
                        }
                    }
                }
                
                // 创建文件夹数据
                Map<String, Object> folderData = new HashMap<>();
                folderData.put("data", imageList);
                folderData.put("count", imageList.size());
                
                result.put(folderName, folderData);
                log.info("[图片管理] 文件夹 {} 包含 {} 个图片", folderName, imageList.size());
            }
        }
        
        return result;
    }
    
    /**
     * 从 JSON 文件中加载图片信息
     * @param folder 文件夹
     * @return 图片名称 -> 图片信息的映射
     */
    private Map<String, Object> loadImageInfoFromJson(File folder) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("[图片管理] 开始加载 JSON 文件，文件夹: {}", folder.getName());
            
            // 查找 JSON 文件
            File jsonFile = null;
            File[] files = folder.listFiles();
            if (files != null) {
                log.info("[图片管理] 文件夹中有 {} 个文件", files.length);
                for (File file : files) {
                    log.debug("[图片管理] 检查文件: {}", file.getName());
                    if (file.isFile() && file.getName().endsWith(".json")) {
                        jsonFile = file;
                        log.info("[图片管理] 找到 JSON 文件: {}", file.getName());
                        break;
                    }
                }
            }
            
            if (jsonFile == null) {
                log.warn("[图片管理] 文件夹 {} 中没有找到 JSON 文件", folder.getName());
                return result;
            }
            
            // 读取 JSON 文件
            String jsonContent = new String(java.nio.file.Files.readAllBytes(jsonFile.toPath()), "UTF-8");
            Map<String, Object> jsonData = parseJson(jsonContent);
            
            if (jsonData == null || !jsonData.containsKey("results")) {
                log.warn("[图片管理] JSON 文件格式不正确: {}", jsonFile.getName());
                return result;
            }
            
            // 提取图片信息
            List<Map<String, Object>> results = (List<Map<String, Object>>) jsonData.get("results");
            for (Map<String, Object> item : results) {
                String imageName = (String) item.get("image_name");
                if (imageName != null) {
                    // 保留原始的图片信息，但不覆盖已有的字段
                    Map<String, Object> info = new HashMap<>(item);
                    result.put(imageName, info);
                    log.debug("[图片管理] 加载 JSON 信息: {}", imageName);
                }
            }
            
            log.info("[图片管理] 从 JSON 文件加载了 {} 个图片信息", result.size());
            
        } catch (Exception e) {
            log.error("[图片管理] 读取 JSON 文件失败", e);
        }
        
        return result;
    }
    
    /**
     * 解析 JSON 字符串
     * @param jsonContent JSON 内容
     * @return 解析后的 Map
     */
    private Map<String, Object> parseJson(String jsonContent) {
        try {
            // 尝试使用 fastjson
            try {
                Class<?> jsonClass = Class.forName("com.alibaba.fastjson.JSON");
                java.lang.reflect.Method parseMethod = jsonClass.getMethod("parseObject", String.class);
                Object result = parseMethod.invoke(null, jsonContent);
                log.info("[图片管理] 使用 fastjson 成功解析 JSON");
                return (Map<String, Object>) result;
            } catch (Exception e1) {
                log.debug("[图片管理] fastjson 不可用，尝试使用 Jackson");
            }
            
            // 尝试使用 Jackson
            try {
                Class<?> objectMapperClass = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
                Object mapper = objectMapperClass.getDeclaredConstructor().newInstance();
                java.lang.reflect.Method readValueMethod = objectMapperClass.getMethod("readValue", String.class, Class.class);
                Object result = readValueMethod.invoke(mapper, jsonContent, Map.class);
                log.info("[图片管理] 使用 Jackson 成功解析 JSON");
                return (Map<String, Object>) result;
            } catch (Exception e2) {
                log.debug("[图片管理] Jackson 不可用，尝试使用 Gson");
            }
            
            // 尝试使用 Gson
            try {
                Class<?> gsonClass = Class.forName("com.google.gson.Gson");
                Object gson = gsonClass.getDeclaredConstructor().newInstance();
                java.lang.reflect.Method fromJsonMethod = gsonClass.getMethod("fromJson", String.class, Class.class);
                Object result = fromJsonMethod.invoke(gson, jsonContent, Map.class);
                log.info("[图片管理] 使用 Gson 成功解析 JSON");
                return (Map<String, Object>) result;
            } catch (Exception e3) {
                log.debug("[图片管理] Gson 不可用");
            }
            
            log.error("[图片管理] 没有可用的 JSON 解析库");
            return null;
            
        } catch (Exception e) {
            log.error("[图片管理] JSON 解析失败", e);
            return null;
        }
    }
    
    /**
     * 检查是否是图片文件
     * @param fileName 文件名
     * @return 是否是图片文件
     */
    private boolean isImageFile(String fileName) {
        String lowerFileName = fileName.toLowerCase();
        return lowerFileName.endsWith(".jpg") ||
               lowerFileName.endsWith(".jpeg") ||
               lowerFileName.endsWith(".png") ||
               lowerFileName.endsWith(".gif") ||
               lowerFileName.endsWith(".bmp") ||
               lowerFileName.endsWith(".webp") ||
               lowerFileName.endsWith(".svg");
    }
    
    /**
     * 递归扫描目录中的所有图片文件
     * @param dir 要扫描的目录
     * @param relativePath 相对路径
     * @param imageList 图片列表
     */
    private void scanImageFiles(File dir, String relativePath, List<Map<String, Object>> imageList) {
        log.info("[图片管理] 开始扫描目录: {}, 相对路径: {}", dir.getAbsolutePath(), relativePath);
        
        if (!dir.exists()) {
            log.warn("[图片管理] 目录不存在: {}", dir.getAbsolutePath());
            return;
        }
        
        if (!dir.isDirectory()) {
            log.warn("[图片管理] 不是目录: {}", dir.getAbsolutePath());
            return;
        }
        
        File[] files = dir.listFiles();
        if (files == null) {
            log.warn("[图片管理] 无法列出文件: {}", dir.getAbsolutePath());
            return;
        }
        
        if (files.length == 0) {
            log.info("[图片管理] 目录为空: {}", dir.getAbsolutePath());
            return;
        }
        
        log.info("[图片管理] 找到 {} 个文件/目录", files.length);
        
        for (File file : files) {
            String currentRelativePath = relativePath.isEmpty() ? file.getName() : relativePath + "/" + file.getName();
            
            if (file.isDirectory()) {
                // 递归扫描子目录
                log.debug("[图片管理] 扫描子目录: {}", currentRelativePath);
                scanImageFiles(file, currentRelativePath, imageList);
            } else if (file.isFile()) {
                String fileName = file.getName();
                
                // 检查是否是图片文件
                if (isImageFile(fileName)) {
                    Map<String, Object> imageInfo = new HashMap<>();
                    imageInfo.put("name", fileName);
                    imageInfo.put("path", currentRelativePath);  // 相对路径
                    imageInfo.put("size", file.length());
                    imageInfo.put("lastModified", file.lastModified());
                    
                    // 构建访问 URL
                    // 前端可以通过 /img/{path}/{filename} 访问图片
                    imageInfo.put("url", "/img/" + currentRelativePath);
                    
                    // 构建完整的访问 URL（包含服务器地址）
                    imageInfo.put("fullUrl", "http://localhost:" + serverPort + "/img/" + currentRelativePath);
                    
                    imageList.add(imageInfo);
                    
                    log.debug("[图片管理] 找到图片文件: {}, 大小: {} bytes", currentRelativePath, file.length());
                }
            }
        }
    }
    
    /**
     * 查找 img 文件夹
     * 尝试多种方式查找，优先级如下：
     * 1. 配置文件中指定的路径 (image.path)
     * 2. user.dir/img (当前工作目录)
     * 3. user.dir/../img (当前工作目录的父目录，用于 target 目录启动)
     * 4. JAR 文件同级目录/img
     * 5. JAR 文件同级目录的父目录/img
     * @return img 文件夹
     */
    private File findImageDirectory() {
        // 方式 1: 尝试使用配置文件中指定的路径
        if (imagePath != null && !imagePath.isEmpty()) {
            File configuredDir = new File(imagePath);
            // 如果是相对路径，转换为绝对路径
            if (!configuredDir.isAbsolute()) {
                configuredDir = new File(System.getProperty("user.dir"), imagePath);
            }
            if (configuredDir.exists() && configuredDir.isDirectory()) {
                log.info("[图片管理] 使用配置文件中指定的图片目录: {}", configuredDir.getAbsolutePath());
                return configuredDir;
            }
        }
        
        // 方式 2: 尝试 user.dir/img
        File imgDir = new File(System.getProperty("user.dir"), "img");
        if (imgDir.exists() && imgDir.isDirectory()) {
            log.info("[图片管理] 在 user.dir 中找到 img 文件夹: {}", imgDir.getAbsolutePath());
            return imgDir;
        }
        
        // 方式 3: 尝试 user.dir/../img (处理从 target 目录启动的情况)
        try {
            File parentDir = new File(System.getProperty("user.dir")).getParentFile();
            if (parentDir != null) {
                imgDir = new File(parentDir, "img");
                if (imgDir.exists() && imgDir.isDirectory()) {
                    log.info("[图片管理] 在 user.dir 的父目录中找到 img 文件夹: {}", imgDir.getAbsolutePath());
                    return imgDir;
                }
            }
        } catch (Exception e) {
            log.debug("[图片管理] 查找 user.dir 父目录失败", e);
        }
        
        // 方式 4: 尝试 JAR 文件同级目录/img
        File jarDir = getJarDirectory();
        if (jarDir != null) {
            imgDir = new File(jarDir, "img");
            if (imgDir.exists() && imgDir.isDirectory()) {
                log.info("[图片管理] 在 JAR 同级目录中找到 img 文件夹: {}", imgDir.getAbsolutePath());
                return imgDir;
            }
            
            // 方式 5: 尝试 JAR 同级目录的父目录/img
            try {
                File jarParentDir = jarDir.getParentFile();
                if (jarParentDir != null) {
                    imgDir = new File(jarParentDir, "img");
                    if (imgDir.exists() && imgDir.isDirectory()) {
                        log.info("[图片管理] 在 JAR 同级目录的父目录中找到 img 文件夹: {}", imgDir.getAbsolutePath());
                        return imgDir;
                    }
                }
            } catch (Exception e) {
                log.debug("[图片管理] 查找 JAR 同级目录的父目录失败", e);
            }
        }
        
        // 方式 6: 返回默认路径（即使不存在）
        return new File(System.getProperty("user.dir"), "img");
    }
    
    /**
     * 获取 JAR 文件所在的目录
     * @return JAR 文件所在的目录
     */
    private File getJarDirectory() {
        try {
            String jarPath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            jarPath = URLDecoder.decode(jarPath, StandardCharsets.UTF_8.name());
            
            File jarFile = new File(jarPath);
            if (jarFile.isFile()) {
                // 如果是 JAR 文件，返回其父目录
                return jarFile.getParentFile();
            } else if (jarFile.isDirectory()) {
                // 如果是目录（开发环境），返回该目录
                return jarFile;
            }
        } catch (Exception e) {
            log.warn("[图片管理] 获取 JAR 目录失败", e);
        }
        return null;
    }
}
