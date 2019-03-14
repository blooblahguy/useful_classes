<?php

	namespace Bloo;
	class SchemaException extends Exception {}
	class MySQLSchema {
		private $structure = array();
		private $db;

		// custom variables
		public $hashtable = "bmschema_hash";
		public $key_length = 7;

		public function __construct($connection = array(), $options = array()) {
			if (! count($connection)) {
				throw new SchemaException("No database provided in class instantiation");
				return;
			}

			// Merge defaults with provided connection information
			$defaults = array(
				"dbname" => "",
				"host" => ini_get("mysqli.default_host"),
				"username" => ini_get("mysqli.default_user"), 
				"password" => ini_get("mysqli.default_pw"), 
				"port" => ini_get("mysqli.default_port"), 
				"socket" => ini_get("mysqli.default_socket")
			);
			$connection = array_merge($defaults, $connection);
			$db = new mysqli($connection['host'], $connection['user'], $connection['password'], $connection['dbname'], $connection['port'], $connection['socket']);
			if (! count($connection)) {
				throw new SchemaException("Error connecting to database");
				return;
			}

			// Merge options into variables
			foreach ($options as $k => $v) {
				$this->{$k} = $v;
			}

			// assign database variable
			$this->db = $db;
		}

		// Query wrapper function to keep consistent debugging and allow for fetchAll
		private function query($qry, $returnAll = false, $primary = false) {
			$rs = $this->db->query($qry) or die($this->db->error);
			if ($returnAll) {
				$all = array();
				while ($t = $rs->fetch_assoc()) {
					if ($primary) {
						$all[$t[$primary]] = $t;
					} else {
						$all[] = $t;
					}
				}
				$rs = $all;
			}

			return $rs;
		}

		// MAIN FUNCTION
		function run() {
			// Create a hashtable and decide whether we need to update things
			$qry = "CREATE TABLE IF NOT EXISTS `{$this->hashtable}`(
				`table_name` VARCHAR(100) NOT NULL
				, `change_hash` VARCHAR(100) NOT NULL
				, UNIQUE(table_name)
			) ";
			$this->db->query($qry) or die($this->db->error);

			// get list of hashes
			$hashes = $this->query("SELECT * FROM {$this->hashtable}", true, "table_name");
			$drop = array();

			foreach ($this->structure as $table => $fields) {
				$change_hash = hashArray($fields);

				if (! isset($hashes[$table]) ) {
					// Create the table inside of the database
					$cmd = "CREATE TABLE IF NOT EXISTS `$table` (id INT({$this->key_length}) UNSIGNED NOT NULL AUTO_INCREMENT, ";
			
					// loop and add custom fields
					foreach ($fields as $name => $type) {
						$type = explode(":",$type);
						$cmd .= "`{$name}` {$type[0]} NOT NULL {$type[1]}, ";
					}

					// always have these fields
					$cmd .= "`created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ";
					$cmd .= "`modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, ";
					$cmd .= "UNIQUE KEY id (id)); ";

					// hash table
					$cmd .= "INSERT INTO {$this->hashtable} (table_name, change_hash) 
					SELECT '{$table}', '{$change_hash}' FROM DUAL
					WHERE NOT EXISTS (
						SELECT table_name FROM {$this->hashtable} WHERE table_name = '{$table}'
					) LIMIT 1;";

					// create this table with fields
					$this->query($cmd);
					
				} elseif ($hashes[$table] !== $change_hash) {
					// unset so we know what to drop when we're finished
					unset($hashes[$table]);

					// Update table structure if needed, ignore key and modified/created since they are automatically generated
					$all_fields = $this->query("SHOW COLUMNS FROM {$table} ", true, "Field");
					unset($all_fields['id']);
					unset($all_fields['created']);
					unset($all_fields['modified']);
					
					$cmd = "";

					// loop and add custom fields
					foreach ($fields as $name => $type) {
						// allows for additional parameters seperated by :
						list($type, $extra) = explode(":", $type);

						// shows that we matched up a local column with a database column
						unset($all_fields[$name]);

						// we found a new column
						if (! isset($all_fields[$name])) {
							$cmd .= "ALTER TABLE `{$table}` ADD `{$name}` {$type} {$extra} NOT NULL; ";
						} else {
							// lets check that the field type and defaults are the same
							$db_type = strtolower($all_fields['Name']['Type']);
							$db_dfex = strtolower($all_fields['Name']['Default'] . $all_fields['Name']['Extra']);
							if (strtolower($type) !== $db_type || strtolower($extra) !== $db_dfex) {
								$cmd .= "ALTER TABLE `{$table}` MODIFY COLUMN `{$name}` {$type} NOT NULL {$extra}; ";
							}
						}					
					}
					
					// anything left over should be dropped as a column
					foreach ($all_fields as $name => $values) {
						$cmd .= "ALTER TABLE `$table` DROP column `$name`;";
					}

					// now update change hash, so that we only hit this function when we've updated the $schema
					$cmd .= "UPDATE {$this->hashtable} SET change_hash = '{$change_hash}' WHERE table_name = '{$table}'; ";
					$this->query($cmd);
				}
			}

			// Take what's remaining in the change hash table and drop it, since it's been removed from the user's schema array
			$cmd = "";
			foreach ($hashes as $table => $fields) {
				$cmd .= "DROP TABLE IF EXISTS `{$table}`;";
			}
			$this->query($cmd);
		}

		// Set which tables should be updated & managed
		function addTable($table, $structure) {
			$this->structure[$table] = $structure;
		}

		// returns a changehash for an array, to decide if it's changed or not
		private function hashArray($array) {
			return md5(serialize($array));
		}

	}
?>