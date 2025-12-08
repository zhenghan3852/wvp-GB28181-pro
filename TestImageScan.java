import java.io.File;

public class TestImageScan {
    public static void main(String[] args) {
        File imgDir = new File("/Users/lihb/Desktop/obj/imgs");
        
        System.out.println("目录存在: " + imgDir.exists());
        System.out.println("是目录: " + imgDir.isDirectory());
        System.out.println("绝对路径: " + imgDir.getAbsolutePath());
        
        File[] files = imgDir.listFiles();
        if (files != null) {
            System.out.println("文件数量: " + files.length);
            for (File file : files) {
                System.out.println("  - " + file.getName() + " (目录: " + file.isDirectory() + ")");
                
                if (file.isDirectory()) {
                    File[] subFiles = file.listFiles();
                    if (subFiles != null) {
                        System.out.println("    子文件数量: " + subFiles.length);
                        for (File subFile : subFiles) {
                            System.out.println("      - " + subFile.getName() + " (文件: " + subFile.isFile() + ", 大小: " + subFile.length() + ")");
                        }
                    }
                }
            }
        } else {
            System.out.println("无法列出文件");
        }
    }
}
