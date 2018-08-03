/*
Navicat MySQL Data Transfer

Source Server         : 10.7.31.13
Source Server Version : 50717
Source Host           : 10.7.31.13:3306
Source Database       : cluster_monitor

Target Server Type    : MYSQL
Target Server Version : 50717
File Encoding         : 65001

Date: 2018-08-03 14:26:09
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for yarn_job_cost
-- ----------------------------
DROP TABLE IF EXISTS `yarn_job_cost`;
CREATE TABLE `yarn_job_cost` (
  `job_id` varchar(200) DEFAULT NULL,
  `job_cost_time` varchar(200) DEFAULT NULL,
  `job_name` varchar(200) DEFAULT NULL,
  `job_detail` text,
  `importdate` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
