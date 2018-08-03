/*
Navicat MySQL Data Transfer

Source Server         : 10.7.31.13
Source Server Version : 50717
Source Host           : 10.7.31.13:3306
Source Database       : cluster_monitor

Target Server Type    : MYSQL
Target Server Version : 50717
File Encoding         : 65001

Date: 2018-08-03 14:26:15
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for yarn_monitor
-- ----------------------------
DROP TABLE IF EXISTS `yarn_monitor`;
CREATE TABLE `yarn_monitor` (
  `total` int(11) DEFAULT NULL,
  `date` varchar(200) DEFAULT NULL,
  `importdate` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
