CREATE TABLE IF NOT EXISTS `pubmed` (
  `id` INT NOT NULL,
  `title` TEXT NOT NULL,
  `journal` TEXT NULL,
  `date` VARCHAR(10) NULL,
  `pub_year` SMALLINT NULL,
  `authors` TEXT NULL,
  `abstract` MEDIUMTEXT NULL,
  `fetch_date` DATETIME NULL,
  `source_file` VARCHAR(255) NULL,
  PRIMARY KEY (`id`),
  KEY `pubmed_year_idx` (`pub_year`, `id`),
  KEY `pubmed_date_idx` (`date`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `pubmed_mirror_file_state` (
  `archive_name` VARCHAR(255) NOT NULL,
  `archive_group` VARCHAR(32) NOT NULL,
  `remote_last_modified` DATETIME NULL,
  `md5` VARCHAR(64) NULL,
  `downloaded_at` DATETIME NULL,
  `processed_at` DATETIME NULL,
  `status` VARCHAR(32) NOT NULL,
  `error_message` TEXT NULL,
  PRIMARY KEY (`archive_name`),
  KEY `pubmed_mirror_group_status_idx` (`archive_group`, `status`, `processed_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
