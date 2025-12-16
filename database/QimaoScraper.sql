SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for Seven_Cats_Novel_Data
-- ----------------------------
DROP TABLE IF EXISTS `Seven_Cats_Novel_Data`;
CREATE TABLE `Seven_Cats_Novel_Data`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '自增主键（物理主键）',
  `book_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '小说唯一标识（来源站点ID）',
  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '小说标题',
  `author` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '作者名称',
  `category1_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '一级分类（如玄幻、都市、言情）',
  `category2_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '二级分类（细分题材标签）',
  `words_num` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '字数（原始字符串，如 120万字）',
  `intro` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '小说简介',
  `image_link` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '封面图片URL',
  `status` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '连载状态（连载中 / 已完结）',
  `number` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '榜单原始数值（字符串）',
  `unit` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '榜单单位（万 / 亿 等）',
  `rank_name` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '榜单名称（人气榜 / 推荐榜 等）',
  `date_type` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '榜单类型（日榜 / 周榜 / 月榜）',
  `date_month` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '榜单月份（yyyy-MM）',
  `rank_date` date NULL DEFAULT NULL COMMENT '榜单统计日期（用于时间分析）',
  `score` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '评分（字符串形式）',
  `read_count` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '阅读量（字符串）',
  `popularity` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '人气值（字符串）',
  `error` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '抓取异常标志（0/1）',
  `error_msg` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '抓取异常信息',
  `gender_type` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '性别向：male / female',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '抓取时间',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_book_gender`(`book_id` ASC, `gender_type` ASC) USING BTREE,
  INDEX `idx_gender_cat1`(`gender_type` ASC, `category1_name` ASC) USING BTREE,
  INDEX `idx_rank_time`(`rank_name` ASC, `rank_date` ASC) USING BTREE,
  INDEX `idx_rank_month`(`rank_name` ASC, `date_month` ASC) USING BTREE,
  INDEX `idx_rank_date`(`rank_date` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 15892 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '小说统一事实表（男女频合并，支持多榜单多时间，Spark/Hive友好）' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of Seven_Cats_Novel_Data
-- ----------------------------
INSERT INTO `Seven_Cats_Novel_Data` VALUES (1, '1924828', '还不起人情债，我只好当她男朋友了', '无色', '都市', '都市生活', '174.83万字', '本书又名：《离婚后，我与美女同事做了临时夫妻》、《离婚后，我的桃花运来了》。\n---\n离婚的那天晚上，江风被朋友接到了他家里喝酒，嫂子做了一桌子丰盛的菜。\n酒过三巡，大家都有些醉意的时候，朋友突然道：“江风，我给你介绍个女朋友吧？包你满意！”', 'https://cdn.qimao.com/bookimg/zww/upload/readerCover/653/1958718_360x480.jpg?time=1740125957', '0', '227.5', '万', '大热', '月榜', '202310', '2023-10-01', '9.0', '4.6', '145.7', 'false', '', 'male', '2025-12-15 12:20:05', '2025-12-15 12:20:05');
