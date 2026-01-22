<template>
  <div id="imageGallery" style="width: 100%; padding-top: 10px">
    <el-button
      icon="el-icon-refresh-right"
      circle
      size="mini"
      @click="loadImages"
    ></el-button>
    <!-- 加载状态 -->

    <div v-if="loading" style="text-align: center; padding: 2rem">
      <el-spinner></el-spinner>
      <p>加载中...</p>
    </div>

    <!-- 图片列表 -->
    <div v-else-if="Object.keys(imageList).length > 0" class="image-gallery">
      <div v-for="(el, key, index) in imageList" :key="key + 'kay'">
        <h3 style="text-align: start">{{ key }}</h3>
        <div class="channel-item">
          <swiper
            class="swiper"
            :options="getSwiperOptions(index)"
            :ref="`swiper${index}`"
          >
            <swiper-slide
              v-for="(image, imgIndex) in el.data"
              :key="imgIndex + 'slide'"
              class="slide"
            >
              <div class="image-preview">
                <img
                  :src="image.url"
                  :alt="image.name"
                  @click="showImageDetail(key, index)"
                />
              </div>
            </swiper-slide>
          </swiper>
        </div>
      </div>
    </div>

    <!-- 空状态 -->
    <div v-else style="text-align: center; padding: 2rem">
      <p>暂无图片</p>
    </div>

    <!-- 图片详情对话框 -->
    <el-dialog
      title="图片详情"
      :visible.sync="dialogVisible"
      width="80%"
      @close="dialogVisible = false"
    >
      <div v-if="selectedImage" style="text-align: center">
        <div
          style="
            display: flex;
            justify-content: center;
            align-items: center;
            margin-bottom: 1rem;
          "
        >
          <el-button
            icon="el-icon-arrow-left"
            circle
            size="mini"
            :disabled="selectedImageIndex === 0"
            @click="
              selectedImageIndex > 0 &&
                (selectedImageIndex--,
                (selectedImage = selectedImageList[selectedImageIndex]))
            "
            style="margin-right: 20px"
          ></el-button>
          <span style="font-size: 14px">
            {{ selectedImageIndex + 1 }} / {{ selectedImageList.length }}
          </span>
          <el-button
            icon="el-icon-arrow-right"
            circle
            size="mini"
            :disabled="selectedImageIndex === selectedImageList.length - 1"
            @click="
              selectedImageIndex < selectedImageList.length - 1 &&
                (selectedImageIndex++,
                (selectedImage = selectedImageList[selectedImageIndex]))
            "
            style="margin-left: 20px"
          ></el-button>
        </div>
        <img
          :src="selectedImage.url"
          style="max-width: 100%; max-height: 600px"
        />
        <div style="margin-top: 1rem">
          <p v-for="(el, key) in selectedImage.algorithm_result" :key="'dialog'+ key">
            <strong>{{ key }}:</strong> {{ el }}
          </p>
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<script>
export default {
  name: "ImageGallery",
  data() {
    return {
      baseSwiperOptions: {
        slidesPerView: 5,
        spaceBetween: 10,
        speed: 800,
        autoplay: false, // 初始关闭，后续手动启动
        pagination: {
          el: ".swiper-pagination",
          clickable: true,
        },
      },
      imageList: [],
      loading: false,
      dialogVisible: false,
      selectedImage: null,
      swiperTimers: [], // 存储定时器，用于清理
    };
  },
  mounted() {
    this.loadImages();
    this._imageInterval = setInterval(() => {
      this.loadImages();
    }, 30000);
  },
  beforeDestroy() {
    // 清理所有定时器
    this.swiperTimers.forEach((timer) => clearTimeout(timer));
    this.swiperTimers = [];
    clearTimeout(this._imageInterval);

    // 停止所有 swiper 自动播放
    Object.keys(this.imageList).forEach((key, index) => {
      const swiperRef = this.$refs[`swiper${index}`];
      if (swiperRef && swiperRef[0] && swiperRef[0].$swiper) {
        swiperRef[0].$swiper.autoplay.stop();
      }
    });
  },
  methods: {
    /**
     * 获取 Swiper 配置（每个实例独立配置）
     */
    getSwiperOptions(index) {
      return {
        ...this.baseSwiperOptions,
      };
    },

    /**
     * 启动错峰自动播放
     */
    startStaggeredAutoplay() {
      const autoplayConfig = {
        delay: 3000,
        stopOnLastSlide: false,
        disableOnInteraction: false,
      };

      // 清理旧的定时器
      this.swiperTimers.forEach((timer) => clearTimeout(timer));
      this.swiperTimers = [];

      // 为每个 swiper 设置延时启动
      Object.keys(this.imageList).forEach((key, index) => {
        const delay = index * 500; // 每个延时 500ms (0.5秒)
        const timer = setTimeout(() => {
          const swiperRef = this.$refs[`swiper${index}`];
          if (swiperRef && swiperRef[0] && swiperRef[0].$swiper) {
            const swiper = swiperRef[0].$swiper;
            swiper.params.autoplay = autoplayConfig;
            swiper.autoplay.start();
            console.log(`Swiper ${index} 启动，延时: ${delay}ms`);
          }
        }, delay);

        this.swiperTimers.push(timer);
      });
    },

    /**
     * 将图片数组分组，每组指定数量
     */
    getImageChunks(images, chunkSize) {
      const chunks = [];
      for (let i = 0; i < images.length; i += chunkSize) {
        chunks.push(images.slice(i, i + chunkSize));
      }
      return chunks;
    },

    /**
     * 鼠标进入轮播区域 - 停止自动播放
     */
    handleMouseEnter(folderName) {
      console.log("鼠标进入:", folderName);
    },

    /**
     * 鼠标离开轮播区域 - 恢复自动播放
     */
    handleMouseLeave(folderName) {
      console.log("鼠标离开:", folderName);
    },

    /**
     * 加载图片列表
     */
    loadImages() {
      this.loading = true;
      this.$axios({
        method: "get",
        url: "/api/image/list",
      })
        .then((res) => {
          console.log("🚀 ~ res:", res);
          if (res.data.data.code === 200) {
            this.imageList = res.data.data.data;
            // 按 lastModified 时间降序排序
            Object.keys(this.imageList).forEach((key) => {
              this.imageList[key].data.sort(
                (a, b) => b.lastModified - a.lastModified
              );
            });
            // 等待 DOM 更新后启动错峰播放
            this.$nextTick(() => {
              this.startStaggeredAutoplay();
            });

            this.$message.success({
              showClose: true,
              message: `成功加载`,
            });
          } else {
            this.$message.error({
              showClose: true,
              message: res.data.message || "加载图片失败",
            });
          }
        })
        .catch((error) => {
          this.$message.error({
            showClose: true,
            message: "加载图片失败: " + error.message,
          });
        })
        .finally(() => {
          this.loading = false;
        });
    },

    /**
     * 显示图片详情
     */
    showImageDetail(key, index) {
      // 支持放大后左右切换
      const images = this.imageList[key].data;
      this.selectedImageIndex = index;
      this.selectedImageList = images;
      this.selectedImage = images[this.selectedImageIndex];
      this.dialogVisible = true;

      // 监听键盘左右键
      if (!this._dialogKeyListener) {
        this._dialogKeyListener = (e) => {
          if (!this.dialogVisible) return;
          if (e.key === "ArrowLeft") {
            if (this.selectedImageIndex > 0) {
              this.selectedImageIndex--;
              this.selectedImage =
                this.selectedImageList[this.selectedImageIndex];
            }
          } else if (e.key === "ArrowRight") {
            if (this.selectedImageIndex < this.selectedImageList.length - 1) {
              this.selectedImageIndex++;
              this.selectedImage =
                this.selectedImageList[this.selectedImageIndex];
            }
          }
        };
        window.addEventListener("keydown", this._dialogKeyListener);
      }
    },

    /**
     * 下载图片
     */
    downloadImage(image) {
      const link = document.createElement("a");
      link.href = image.url;
      link.download = image.name;
      link.click();
    },

    /**
     * 格式化文件大小
     */
    formatFileSize(bytes) {
      if (bytes === 0) return "0 B";
      const k = 1024;
      const sizes = ["B", "KB", "MB", "GB"];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + " " + sizes[i];
    },

    /**
     * 格式化时间
     */
    formatTime(timestamp) {
      const date = new Date(timestamp);
      return date.toLocaleString("zh-CN");
    },
  },
};
</script>

<style scoped>
.image-gallery-container {
  padding: 1rem;
}

.folder-section {
  margin-bottom: 2rem;
}

.folder-header {
  margin-bottom: 1rem;
  padding-bottom: 0.5rem;
  border-bottom: 2px solid #409eff;
}

.folder-header h3 {
  margin: 0;
  color: #409eff;
  font-size: 16px;
}

.image-gallery {
  gap: 1rem;
  padding: 0;
}
.channel-item {
  margin-top: 20px;
  overflow: auto;
  width: 100%;
  display: flex;
  gap: 10px;
}

.image-item {
  min-width: 300px;
  width: 20%;
  flex-shrink: 0;
  border-radius: 4px;
  overflow: hidden;
  transition: all 0.3s ease;
}

.image-item:hover {
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
  transform: translateY(-2px);
}

.image-preview {
  width: 100%;
  height: 100%;
  overflow: hidden;
  display: flex;
  align-items: center;
  justify-content: center;
}

.image-preview img {
  min-width: 100%;
  min-height: 100%;
  object-fit: cover;
  cursor: pointer;
  object-fit: contain;
}

.image-info {
  padding: 0.75rem;
}

.image-name {
  margin: 0 0 0.5rem 0;
  font-weight: bold;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 14px;
}

.image-size {
  margin: 0.25rem 0;
  color: #666;
  font-size: 12px;
}

.image-time {
  margin: 0.25rem 0 0.75rem 0;
  color: #999;
  font-size: 12px;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem;
  background-color: #f5f5f5;
  border-bottom: 1px solid #ddd;
}

.page-title {
  font-size: 18px;
  font-weight: bold;
}

.page-header-btn {
  display: flex;
  gap: 0.5rem;
}
.swiper {
  width: 100%;
  padding: 10px 0 10px;
}

/* Slide 样式 */
.slide {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
}

/* 分页器容器样式 */
.swiper >>> .swiper-pagination {
  bottom: 10px;
}

/* 分页器圆点样式 */
.swiper >>> .swiper-pagination-bullet {
  width: 10px;
  height: 10px;
  background: #000;
  opacity: 0.3;
  border-radius: 50%;
  cursor: pointer;
  transition: all 0.3s;
}

/* 激活状态的分页器圆点 */
.swiper >>> .swiper-pagination-bullet-active {
  opacity: 1;
  background: #007aff;
}

/* 鼠标悬停效果 */
.swiper >>> .swiper-pagination-bullet:hover {
  opacity: 0.6;
}
</style>