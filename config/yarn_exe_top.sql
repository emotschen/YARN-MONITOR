/*
Navicat MySQL Data Transfer

Source Server         : 10.7.31.13
Source Server Version : 50717
Source Host           : 10.7.31.13:3306
Source Database       : cluster_monitor

Target Server Type    : MYSQL
Target Server Version : 50717
File Encoding         : 65001

Date: 2018-08-03 14:26:01
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for yarn_exe_top
-- ----------------------------
DROP TABLE IF EXISTS `yarn_exe_top`;
CREATE TABLE `yarn_exe_top` (
  `job_id` varchar(200) DEFAULT NULL,
  `job_name` varchar(200) DEFAULT NULL,
  `job_exec_frequency` int(11) DEFAULT NULL,
  `importdate` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
