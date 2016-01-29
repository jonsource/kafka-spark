CREATE TABLE `kafka_offsets` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT, 
  `task` varchar(16) NOT NULL,
  `topic` varchar(64) NOT NULL,
  `partition` smallint unsigned NOT NULL,
  `offset` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unq_task_topic_partition` (`task`,`topic`,`partition`),
  KEY `idx_task` (`task`),
  KEY `idx_topic` (`topic`)
) DEFAULT CHARSET=utf8
