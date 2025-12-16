-- ============================================
-- QimaoScraper Feature Data Warehouse
-- 特征数据仓库 - ADS层表结构定义
-- 创建时间: 2025-12-16
-- 描述: 包含8个ADS层特征表的MySQL建表语句
-- ============================================

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS `QimaoScraper_Feature_Data` 
DEFAULT CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE `QimaoScraper_Feature_Data`;

-- ============================================
-- 平台侧 - 2个表
-- ============================================

-- ----------------------------
-- Table structure for ads_platform_heat
-- 平台侧1: 热度分析（一阶导数、二阶导数、冷启动监测）
-- ----------------------------
DROP TABLE IF EXISTS `ads_platform_heat`;
CREATE TABLE `ads_platform_heat` (
  `book_id` VARCHAR(50) NOT NULL COMMENT '小说ID',
  `title` VARCHAR(255) DEFAULT NULL COMMENT '小说标题',
  `author` VARCHAR(100) DEFAULT NULL COMMENT '作者',
  `category1_name` VARCHAR(50) DEFAULT NULL COMMENT '一级分类',
  `category2_name` VARCHAR(50) DEFAULT NULL COMMENT '二级分类',
  `rank_name` VARCHAR(50) DEFAULT NULL COMMENT '榜单名称',
  `rank_date` DATE DEFAULT NULL COMMENT '榜单日期',
  `created_at` TIMESTAMP DEFAULT NULL COMMENT '创建时间',
  `numeric_popularity` DOUBLE DEFAULT NULL COMMENT '当前热度',
  `numeric_read_count` DOUBLE DEFAULT NULL COMMENT '当前阅读量',
  `numeric_score` DOUBLE DEFAULT NULL COMMENT '评分',
  `status` VARCHAR(20) DEFAULT NULL COMMENT '状态',
  `gender_type` VARCHAR(10) DEFAULT NULL COMMENT '性别向',
  `prev_day_popularity` DOUBLE DEFAULT NULL COMMENT '前一天热度',
  `popularity_diff` DOUBLE DEFAULT NULL COMMENT '热度差值（一阶导数）',
  `popularity_growth_rate` DOUBLE DEFAULT NULL COMMENT '热度变化率（环比增速%）',
  `prev_popularity_diff` DOUBLE DEFAULT NULL COMMENT '前一天热度差值',
  `popularity_acceleration` DOUBLE DEFAULT NULL COMMENT '热度加速度（二阶导数）',
  `category_avg_growth_rate` DOUBLE DEFAULT NULL COMMENT '同类平均增长率',
  `is_new_book` INT DEFAULT NULL COMMENT '是否新书（7天内）',
  `is_high_growth` INT DEFAULT NULL COMMENT '是否高增长',
  `is_cold_start_quality` INT DEFAULT NULL COMMENT '是否冷启动优质作品',
  `heat_level` VARCHAR(20) DEFAULT NULL COMMENT '热度等级',
  KEY `idx_book_date` (`book_id`, `rank_date`),
  KEY `idx_rank_date` (`rank_date`),
  KEY `idx_category` (`category1_name`, `gender_type`),
  KEY `idx_heat_level` (`heat_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='平台侧-热度分析表';

-- ----------------------------
-- Table structure for ads_platform_ranking_trend
-- 平台侧2: 榜单趋势分析（德不配位识别、转化率）
-- ----------------------------
DROP TABLE IF EXISTS `ads_platform_ranking_trend`;
CREATE TABLE `ads_platform_ranking_trend` (
  `book_id` VARCHAR(50) NOT NULL COMMENT '小说ID',
  `title` VARCHAR(255) DEFAULT NULL COMMENT '小说标题',
  `author` VARCHAR(100) DEFAULT NULL COMMENT '作者',
  `rank_name` VARCHAR(50) DEFAULT NULL COMMENT '榜单名称',
  `category1_name` VARCHAR(50) DEFAULT NULL COMMENT '一级分类',
  `rank_date` DATE DEFAULT NULL COMMENT '榜单日期',
  `numeric_popularity` DOUBLE DEFAULT NULL COMMENT '热度',
  `numeric_read_count` DOUBLE DEFAULT NULL COMMENT '阅读量',
  `numeric_score` DOUBLE DEFAULT NULL COMMENT '评分',
  `status` VARCHAR(20) DEFAULT NULL COMMENT '状态',
  `gender_type` VARCHAR(10) DEFAULT NULL COMMENT '性别向',
  `prev_read_count` DOUBLE DEFAULT NULL COMMENT '前一期阅读量',
  `read_count_growth` DOUBLE DEFAULT NULL COMMENT '阅读量增长',
  `read_count_growth_rate` DOUBLE DEFAULT NULL COMMENT '阅读量增长率%',
  `ranking_conversion_rate` DOUBLE DEFAULT NULL COMMENT '榜单转化率%',
  `is_unworthy` INT DEFAULT NULL COMMENT '是否德不配位',
  `is_high_conversion` INT DEFAULT NULL COMMENT '是否高转化率',
  `recommend_weight` DOUBLE DEFAULT NULL COMMENT '推荐权重',
  KEY `idx_book_rank_date` (`book_id`, `rank_name`, `rank_date`),
  KEY `idx_rank_name` (`rank_name`),
  KEY `idx_conversion` (`is_high_conversion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='平台侧-榜单趋势分析表';

-- ============================================
-- 作者侧 - 2个表
-- ============================================

-- ----------------------------
-- Table structure for ads_author_reason
-- 作者侧1: 热度原因分析（关键词归因、字数区间）
-- ----------------------------
DROP TABLE IF EXISTS `ads_author_reason`;
CREATE TABLE `ads_author_reason` (
  `category1_name` VARCHAR(50) NOT NULL COMMENT '一级分类',
  `category2_name` VARCHAR(50) NOT NULL COMMENT '二级分类',
  `words_range` VARCHAR(50) NOT NULL COMMENT '字数区间',
  `status` VARCHAR(20) NOT NULL COMMENT '连载状态',
  `has_hot_keywords` INT NOT NULL COMMENT '是否包含热门关键词',
  `is_golden_range` INT NOT NULL COMMENT '是否黄金字数区间',
  `book_count` BIGINT DEFAULT NULL COMMENT '作品数量',
  `avg_popularity` DOUBLE DEFAULT NULL COMMENT '平均热度',
  `avg_score` DOUBLE DEFAULT NULL COMMENT '平均评分',
  `avg_words` DOUBLE DEFAULT NULL COMMENT '平均字数',
  `avg_intro_length` DOUBLE DEFAULT NULL COMMENT '平均简介长度',
  `avg_title_length` DOUBLE DEFAULT NULL COMMENT '平均标题长度',
  `heat_index` DOUBLE DEFAULT NULL COMMENT '热度指数',
  KEY `idx_category` (`category1_name`, `category2_name`),
  KEY `idx_words_range` (`words_range`),
  KEY `idx_keywords` (`has_hot_keywords`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='作者侧-热度原因分析表';

-- ----------------------------
-- Table structure for ads_author_attenuation_effect
-- 作者侧2: 热度衰减效应（完结后衰减曲线）
-- ----------------------------
DROP TABLE IF EXISTS `ads_author_attenuation_effect`;
CREATE TABLE `ads_author_attenuation_effect` (
  `book_id` VARCHAR(50) NOT NULL COMMENT '小说ID',
  `title` VARCHAR(255) DEFAULT NULL COMMENT '小说标题',
  `author` VARCHAR(100) DEFAULT NULL COMMENT '作者',
  `category1_name` VARCHAR(50) DEFAULT NULL COMMENT '一级分类',
  `status` VARCHAR(20) DEFAULT NULL COMMENT '连载状态',
  `gender_type` VARCHAR(10) DEFAULT NULL COMMENT '性别向',
  `peak_popularity` DOUBLE DEFAULT NULL COMMENT '峰值热度',
  `lowest_popularity` DOUBLE DEFAULT NULL COMMENT '最低热度',
  `avg_attenuation_rate` DOUBLE DEFAULT NULL COMMENT '平均衰减率%',
  `avg_attenuation_speed` DOUBLE DEFAULT NULL COMMENT '平均衰减速度%',
  `lifecycle_days` INT DEFAULT NULL COMMENT '生命周期天数',
  `avg_score` DOUBLE DEFAULT NULL COMMENT '平均评分',
  `has_severe_attenuation` INT DEFAULT NULL COMMENT '是否严重衰减',
  `total_attenuation` DOUBLE DEFAULT NULL COMMENT '总衰减幅度%',
  `attenuation_type` VARCHAR(20) DEFAULT NULL COMMENT '衰减类型',
  `recommended_new_book_days` DOUBLE DEFAULT NULL COMMENT '建议推出新书天数',
  PRIMARY KEY (`book_id`),
  KEY `idx_author` (`author`),
  KEY `idx_attenuation_type` (`attenuation_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='作者侧-热度衰减效应表';

-- ============================================
-- 用户侧 - 2个表
-- ============================================

-- ----------------------------
-- Table structure for ads_user_layered_recommendation
-- 用户侧1: 分层推荐（圈层爆款、相对热度）
-- ----------------------------
DROP TABLE IF EXISTS `ads_user_layered_recommendation`;
CREATE TABLE `ads_user_layered_recommendation` (
  `book_id` VARCHAR(50) NOT NULL COMMENT '小说ID',
  `title` VARCHAR(255) DEFAULT NULL COMMENT '小说标题',
  `author` VARCHAR(100) DEFAULT NULL COMMENT '作者',
  `category1_name` VARCHAR(50) DEFAULT NULL COMMENT '一级分类',
  `category2_name` VARCHAR(50) DEFAULT NULL COMMENT '二级分类',
  `gender_type` VARCHAR(10) DEFAULT NULL COMMENT '性别向',
  `numeric_popularity` DOUBLE DEFAULT NULL COMMENT '热度',
  `numeric_score` DOUBLE DEFAULT NULL COMMENT '评分',
  `numeric_read_count` DOUBLE DEFAULT NULL COMMENT '阅读量',
  `status` VARCHAR(20) DEFAULT NULL COMMENT '状态',
  `category_heat_rank` INT DEFAULT NULL COMMENT '分类内热度排名',
  `subcategory_heat_rank` INT DEFAULT NULL COMMENT '细分类内热度排名',
  `category_avg_popularity` DOUBLE DEFAULT NULL COMMENT '分类平均热度',
  `relative_heat` DOUBLE DEFAULT NULL COMMENT '相对热度',
  `is_niche_hit` INT DEFAULT NULL COMMENT '是否圈层爆款',
  `is_high_score_niche` INT DEFAULT NULL COMMENT '是否高分小众',
  `niche_recommendation_score` DOUBLE DEFAULT NULL COMMENT '推荐分（圈层内）',
  `recommendation_level` VARCHAR(20) DEFAULT NULL COMMENT '推荐等级',
  KEY `idx_book` (`book_id`),
  KEY `idx_category_gender` (`category1_name`, `gender_type`),
  KEY `idx_recommendation_level` (`recommendation_level`),
  KEY `idx_niche_hit` (`is_niche_hit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='用户侧-分层推荐表';

-- ----------------------------
-- Table structure for ads_user_avoid_pitfalls
-- 用户侧2: 避坑指南（刷榜识别、高热低分预警）
-- ----------------------------
DROP TABLE IF EXISTS `ads_user_avoid_pitfalls`;
CREATE TABLE `ads_user_avoid_pitfalls` (
  `book_id` VARCHAR(50) NOT NULL COMMENT '小说ID',
  `title` VARCHAR(255) DEFAULT NULL COMMENT '小说标题',
  `author` VARCHAR(100) DEFAULT NULL COMMENT '作者',
  `category1_name` VARCHAR(50) DEFAULT NULL COMMENT '一级分类',
  `category2_name` VARCHAR(50) DEFAULT NULL COMMENT '二级分类',
  `rank_name` VARCHAR(50) DEFAULT NULL COMMENT '榜单名称',
  `gender_type` VARCHAR(10) DEFAULT NULL COMMENT '性别向',
  `numeric_popularity` DOUBLE DEFAULT NULL COMMENT '热度',
  `numeric_score` DOUBLE DEFAULT NULL COMMENT '评分',
  `numeric_read_count` DOUBLE DEFAULT NULL COMMENT '阅读量',
  `rank_date` DATE DEFAULT NULL COMMENT '榜单日期',
  `heat_score_ratio` DOUBLE DEFAULT NULL COMMENT '热度评分比',
  `is_high_heat_low_score` INT DEFAULT NULL COMMENT '是否高热低分',
  `is_suspicious_boost` INT DEFAULT NULL COMMENT '是否可疑刷榜',
  `read_conversion` DOUBLE DEFAULT NULL COMMENT '阅读转化率%',
  `is_low_conversion` INT DEFAULT NULL COMMENT '是否低转化',
  `risk_level` VARCHAR(20) DEFAULT NULL COMMENT '风险等级',
  `recommendation_weight` DOUBLE DEFAULT NULL COMMENT '推荐权重',
  `avoidance_advice` VARCHAR(100) DEFAULT NULL COMMENT '避坑建议',
  KEY `idx_book` (`book_id`),
  KEY `idx_risk_level` (`risk_level`),
  KEY `idx_suspicious` (`is_suspicious_boost`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='用户侧-避坑指南表';

-- ============================================
-- 资本侧 - 2个表
-- ============================================

-- ----------------------------
-- Table structure for ads_capital_ltv
-- 资本侧1: IP长尾价值计算（热度积分累积、改编适配度）
-- ----------------------------
DROP TABLE IF EXISTS `ads_capital_ltv`;
CREATE TABLE `ads_capital_ltv` (
  `book_id` VARCHAR(50) NOT NULL COMMENT '小说ID',
  `title` VARCHAR(255) DEFAULT NULL COMMENT '小说标题',
  `author` VARCHAR(100) DEFAULT NULL COMMENT '作者',
  `category1_name` VARCHAR(50) DEFAULT NULL COMMENT '一级分类',
  `category2_name` VARCHAR(50) DEFAULT NULL COMMENT '二级分类',
  `status` VARCHAR(20) DEFAULT NULL COMMENT '连载状态',
  `gender_type` VARCHAR(10) DEFAULT NULL COMMENT '性别向',
  `total_heat_integral` DOUBLE DEFAULT NULL COMMENT '热度积分（累积热度）',
  `avg_popularity` DOUBLE DEFAULT NULL COMMENT '平均热度',
  `peak_popularity` DOUBLE DEFAULT NULL COMMENT '峰值热度',
  `lowest_popularity` DOUBLE DEFAULT NULL COMMENT '最低热度',
  `appearance_count` BIGINT DEFAULT NULL COMMENT '上榜次数',
  `first_rank_date` DATE DEFAULT NULL COMMENT '首次上榜日期',
  `latest_rank_date` DATE DEFAULT NULL COMMENT '最近上榜日期',
  `avg_score` DOUBLE DEFAULT NULL COMMENT '平均评分',
  `avg_read_count` DOUBLE DEFAULT NULL COMMENT '平均阅读量',
  `total_words` DOUBLE DEFAULT NULL COMMENT '总字数',
  `lifecycle_days` INT DEFAULT NULL COMMENT '生命周期天数',
  `daily_avg_heat` DOUBLE DEFAULT NULL COMMENT '日均热度',
  `heat_volatility` DOUBLE DEFAULT NULL COMMENT '热度波动率',
  `heat_stability` DOUBLE DEFAULT NULL COMMENT '热度稳定性',
  `ip_type` VARCHAR(20) DEFAULT NULL COMMENT 'IP类型',
  `ltv_score` DOUBLE DEFAULT NULL COMMENT 'LTV分数',
  `ip_value_level` VARCHAR(10) DEFAULT NULL COMMENT 'IP价值等级',
  `adaptation_suggestion` VARCHAR(50) DEFAULT NULL COMMENT '改编建议',
  PRIMARY KEY (`book_id`),
  KEY `idx_author` (`author`),
  KEY `idx_ip_value_level` (`ip_value_level`),
  KEY `idx_ip_type` (`ip_type`),
  KEY `idx_ltv_score` (`ltv_score`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='资本侧-IP长尾价值计算表';

-- ----------------------------
-- Table structure for ads_capital_future_purchasing_power
-- 资本侧2: 粉丝粘性与购买力验证（ARPU分析）
-- ----------------------------
DROP TABLE IF EXISTS `ads_capital_future_purchasing_power`;
CREATE TABLE `ads_capital_future_purchasing_power` (
  `book_id` VARCHAR(50) NOT NULL COMMENT '小说ID',
  `title` VARCHAR(255) DEFAULT NULL COMMENT '小说标题',
  `author` VARCHAR(100) DEFAULT NULL COMMENT '作者',
  `category1_name` VARCHAR(50) DEFAULT NULL COMMENT '一级分类',
  `gender_type` VARCHAR(10) DEFAULT NULL COMMENT '性别向',
  `avg_arpu` DOUBLE DEFAULT NULL COMMENT '平均ARPU指标',
  `max_arpu` DOUBLE DEFAULT NULL COMMENT '最大ARPU指标',
  `avg_fan_quality` DOUBLE DEFAULT NULL COMMENT '平均粉丝质量指数',
  `has_payment_rank` INT DEFAULT NULL COMMENT '是否上过付费榜',
  `is_high_arpu_book` INT DEFAULT NULL COMMENT '是否高ARPU作品',
  `avg_score` DOUBLE DEFAULT NULL COMMENT '平均评分',
  `avg_read_count` DOUBLE DEFAULT NULL COMMENT '平均阅读量',
  `avg_popularity` DOUBLE DEFAULT NULL COMMENT '平均热度',
  `appearance_count` BIGINT DEFAULT NULL COMMENT '出现次数',
  `fan_value_score` DOUBLE DEFAULT NULL COMMENT '综合粉丝价值评分',
  `investment_value_level` VARCHAR(10) DEFAULT NULL COMMENT '投资价值等级',
  `investment_recommendation` VARCHAR(50) DEFAULT NULL COMMENT '投资建议',
  PRIMARY KEY (`book_id`),
  KEY `idx_author` (`author`),
  KEY `idx_investment_level` (`investment_value_level`),
  KEY `idx_high_arpu` (`is_high_arpu_book`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='资本侧-粉丝粘性与购买力验证表';

-- ============================================
-- 初始化完成提示
-- ============================================
SELECT 'QimaoScraper Feature Data Warehouse initialized successfully!' AS message;
SELECT 'Created 8 ADS layer tables:' AS message;
SELECT '  [Platform] ads_platform_heat' AS message;
SELECT '  [Platform] ads_platform_ranking_trend' AS message;
SELECT '  [Author] ads_author_reason' AS message;
SELECT '  [Author] ads_author_attenuation_effect' AS message;
SELECT '  [User] ads_user_layered_recommendation' AS message;
SELECT '  [User] ads_user_avoid_pitfalls' AS message;
SELECT '  [Capital] ads_capital_ltv' AS message;
SELECT '  [Capital] ads_capital_future_purchasing_power' AS message;

SET FOREIGN_KEY_CHECKS = 1;
