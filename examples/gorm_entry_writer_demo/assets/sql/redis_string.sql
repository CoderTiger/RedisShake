CREATE TABLE IF NOT EXISTS `redis_string` (
  `key_hash` VARCHAR(64) NOT NULL COMMENT 'SHA256',
  `key` TEXT NOT NULL,
  `value` TEXT NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `key_hash` (`key_hash` ASC) VISIBLE,
  UNIQUE INDEX `key_hash_UNIQUE` (`key_hash` ASC) VISIBLE,
  PRIMARY KEY (`key_hash`)) /*T![ttl] TTL = `created_at` + INTERVAL 7 DAY TTL_ENABLE = 'ON'*/;
  