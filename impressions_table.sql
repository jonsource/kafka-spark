CREATE TABLE `impressions` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `banner_id` int(10) NOT NULL DEFAULT '0',
  `view_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `view_count` int(10) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `unq_bannerid_viewdate` (`banner_id`,`view_date`),
  KEY `idx_bannerid` (`banner_id`),
  KEY `idx_viewdate` (`view_date`)
) DEFAULT CHARSET=utf8
