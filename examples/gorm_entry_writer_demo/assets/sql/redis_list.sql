CREATE TABLE IF NOT EXISTS `redis_list` (
  `key_hash` VARCHAR(64) NOT NULL COMMENT 'SHA256',
  `key` VARCHAR(1024) NOT NULL,
  `position` INT UNSIGNED NOT NULL,
  `value` VARBINARY(10240) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`position`, `key_hash`),
  UNIQUE INDEX `key_hash_UNIQUE` (`key_hash` ASC) VISIBLE)
ENGINE = InnoDB /*T![ttl] TTL = `created_at` + INTERVAL 7 DAY TTL_ENABLE = 'ON'*/;
