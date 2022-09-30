defmodule DaoMysqlFarmersAppTest do
  use ExUnit.Case, async: false

  alias DaoEngine, as: Dao

  test "test 01: adding farms with the same columns, generates a single insert statment" do
    context = initial_context()

    query = [
      add: [
        register_farms: [
          farms: [
            %{
              "name" => "bananas farm",
              "location" => "kampala",
              "width" => 50,
              "height" => 100
            },
            %{
              "name" => "general animal farm",
              "location" => "jinja",
              "width" => 400,
              "height" => 400
            }
          ]
        ]
      ]
    ]

    results = Dao.execute(context, query)
    test1_expected_results = get_test1_expected_results()

    assert test1_expected_results["auto_schema_changes_results"] ==
             results["auto_schema_changes_results"]

    assert test1_expected_results["context"] == results["context"]
    assert test1_expected_results["root_cmd_node_list"] == results["root_cmd_node_list"]
    assert test1_expected_results["root_cmd_sqls_results"] == results["root_cmd_sqls_results"]
  end

  test "test 02: adding farms with the different columns, generates a multiple insert statment" do
    context = initial_context()

    # register farms
    # register farmers
    # a farmer is one who works on a farmer
    # a farmer can be deployed to work on multiple farms
    # record each day that a famer works on a certain farm

    # changing requirement/ update
    # modify the db so that we can record if a farmer owns a certain farm

    description = ~S(
      we can alwyas grow so much food that no one in the world will ever starv again, are my right
      and every human in the whole world wide web is speciall
    )
    # register farms,
    query = [
      add: [
        register_farms: [
          farms: [
            %{
              "name" => "bananas farm",
              "location" => "kampala",
              "width" => 50,
              "height" => 100,
              "size" => "50 x 100",
              "description" => description
            },
            %{
              "name" => "general animal farm",
              "location" => "jinja",
              "width" => 400,
              "height" => 400
            }
          ]
        ]
      ]
    ]

    # nyd: please note the auto convertion to test data type column
    results = Dao.execute(context, query)
    test2_expected_results = get_test2_expected_results()

    assert test2_expected_results["auto_schema_changes_results"] ==
             results["auto_schema_changes_results"]

    assert test2_expected_results["context"] == results["context"]
    assert test2_expected_results["root_cmd_node_list"] == results["root_cmd_node_list"]
    assert test2_expected_results["root_cmd_sqls_results"] == results["root_cmd_sqls_results"]
  end

  test "test 03: fetch a list of farms, adds a status column with default value using short hand" do
    test2_results = get_test2_expected_results()
    context = test2_results["context"]
    context = Map.put(context, "reset_db", false)
    context = Map.put(context, "auto_schema_changes", [])

    # register farms,
    query = [
      get: [
        list_of_farms: [
          farms: %{
            "name" => "str",
            "location" => "str",
            "width" => "int",
            "height" => "int",
            "size" => "str",
            "description" => "str",
            "status" => "str 30 def active"
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    test3_expected_results = get_test3_expected_results()

    assert test3_expected_results["auto_schema_changes_results"] ==
             results["auto_schema_changes_results"]

    assert test3_expected_results["context"] == results["context"]
    assert test3_expected_results["root_cmd_node_list"] == results["root_cmd_node_list"]
    assert test3_expected_results["root_cmd_sqls_results"] == results["root_cmd_sqls_results"]
  end

  defp initial_context() do
    %{
      "database_type" => "mysql",
      "database_name" => "agric",
      "schema" => %{},
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "track_id" => "farmers_app",
      "reset_db" => true,
      "remove_sql_results_connection_ids" => true
    }
  end

  def res() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `farms` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, height INT(30), location VARCHAR(30), name VARCHAR(30), size VARCHAR(30), width INT(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "farmers_app",
        "database_type" => "mysql",
        "remove_sql_results_connection_ids" => true,
        "reset_db" => true,
        "schema" => %{
          "farms" => %{
            "created_at" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "deleted_on" => %{
              "auto_increment" => false,
              "default" => "NULL",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT NULL",
              "type" => "datetime",
              "unique" => false
            },
            "height" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            },
            "id" => %{
              "auto_increment" => true,
              "default" => nil,
              "is_primary_key" => true,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
              "type" => "integer"
            },
            "is_deleted" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => true,
              "size" => 1,
              "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
              "type" => "boolean",
              "unique" => false
            },
            "last_update_on" => %{
              "auto_increment" => false,
              "default" => "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "location" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "size" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "width" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            }
          }
        },
        "track_id" => "farmers_app"
      },
      "root_cmd_node_list" => [
        add: [
          register_farms: [
            farms: %{
              "is_list" => true,
              "sql" =>
                "INSERT INTO `farms`(height, location, name, width) VALUES(400, 'jinja', 'general animal farm', 400); INSERT INTO `farms`(height, location, name, size, width) VALUES(100, 'kampala', 'bananas farm', '50 x 100', 50)"
            }
          ]
        ]
      ]
    }
  end

  def get_test1_expected_results() do
    %{
      "auto_schema_changes_results" => %{
        "errors" => %{},
        "has_errors" => false,
        "index_ref_results" => %{
          0 => %{
            "results" =>
              {:ok,
               %MyXQL.Result{
                 columns: nil,
                 connection_id: :dao@removed,
                 last_insert_id: 0,
                 num_rows: 1,
                 num_warnings: 0,
                 rows: nil
               }},
            "sql" => "DROP DATABASE IF EXISTS agric"
          },
          1 => %{
            "results" =>
              {:ok,
               %MyXQL.Result{
                 columns: nil,
                 connection_id: :dao@removed,
                 last_insert_id: 0,
                 num_rows: 1,
                 num_warnings: 0,
                 rows: nil
               }},
            "sql" => "CREATE DATABASE IF NOT EXISTS agric"
          },
          2 => %{
            "results" =>
              {:ok,
               %MyXQL.Result{
                 columns: nil,
                 connection_id: :dao@removed,
                 last_insert_id: 0,
                 num_rows: 0,
                 num_warnings: 0,
                 rows: nil
               }},
            "sql" =>
              "CREATE TABLE `farms` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, height INT(30), location VARCHAR(30), name VARCHAR(30), width INT(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
          }
        },
        "next_index" => 3
      },
      "connections" => %{"agric" => :any},
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "DROP DATABASE IF EXISTS agric",
          "CREATE DATABASE IF NOT EXISTS agric",
          "CREATE TABLE `farms` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, height INT(30), location VARCHAR(30), name VARCHAR(30), width INT(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "agric",
        "database_type" => "mysql",
        "remove_sql_results_connection_ids" => true,
        "reset_db" => true,
        "schema" => %{
          "farms" => %{
            "created_at" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "deleted_on" => %{
              "auto_increment" => false,
              "default" => "NULL",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT NULL",
              "type" => "datetime",
              "unique" => false
            },
            "height" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            },
            "id" => %{
              "auto_increment" => true,
              "default" => nil,
              "is_primary_key" => true,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
              "type" => "integer"
            },
            "is_deleted" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => true,
              "size" => 1,
              "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
              "type" => "boolean",
              "unique" => false
            },
            "last_update_on" => %{
              "auto_increment" => false,
              "default" => "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "location" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "width" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            }
          }
        },
        "track_id" => "farmers_app"
      },
      "root_cmd_node_list" => [
        add: [
          register_farms: [
            farms: %{
              "is_list" => true,
              "sql" =>
                "INSERT INTO `farms`(height, location, name, width) VALUES(100, 'kampala', 'bananas farm', 50), (400, 'jinja', 'general animal farm', 400)"
            }
          ]
        ]
      ],
      "root_cmd_sqls_results" => %{
        "errors" => %{},
        "has_errors" => false,
        "results" => [
          add: [
            register_farms: [
              farms: %{
                "results" =>
                  {:ok,
                   %MyXQL.Result{
                     columns: nil,
                     connection_id: :dao@removed,
                     last_insert_id: 1,
                     num_rows: 2,
                     num_warnings: 0,
                     rows: nil
                   }},
                "sql" =>
                  "INSERT INTO `farms`(height, location, name, width) VALUES(100, 'kampala', 'bananas farm', 50), (400, 'jinja', 'general animal farm', 400)"
              }
            ]
          ]
        ]
      }
    }
  end

  def get_test2_expected_results() do
    %{
      "auto_schema_changes_results" => %{
        "errors" => %{},
        "has_errors" => false,
        "index_ref_results" => %{
          0 => %{
            "results" =>
              {:ok,
               %MyXQL.Result{
                 columns: nil,
                 connection_id: :dao@removed,
                 last_insert_id: 0,
                 num_rows: 1,
                 num_warnings: 0,
                 rows: nil
               }},
            "sql" => "DROP DATABASE IF EXISTS agric"
          },
          1 => %{
            "results" =>
              {:ok,
               %MyXQL.Result{
                 columns: nil,
                 connection_id: :dao@removed,
                 last_insert_id: 0,
                 num_rows: 1,
                 num_warnings: 0,
                 rows: nil
               }},
            "sql" => "CREATE DATABASE IF NOT EXISTS agric"
          },
          2 => %{
            "results" =>
              {:ok,
               %MyXQL.Result{
                 columns: nil,
                 connection_id: :dao@removed,
                 last_insert_id: 0,
                 num_rows: 0,
                 num_warnings: 0,
                 rows: nil
               }},
            "sql" =>
              "CREATE TABLE `farms` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, description TEXT DEFAULT NULL, height INT(30), location VARCHAR(30), name VARCHAR(30), size VARCHAR(30), width INT(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
          }
        },
        "next_index" => 3
      },
      "connections" => %{"agric" => :any},
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "DROP DATABASE IF EXISTS agric",
          "CREATE DATABASE IF NOT EXISTS agric",
          "CREATE TABLE `farms` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, description TEXT DEFAULT NULL, height INT(30), location VARCHAR(30), name VARCHAR(30), size VARCHAR(30), width INT(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "agric",
        "database_type" => "mysql",
        "remove_sql_results_connection_ids" => true,
        "reset_db" => true,
        "schema" => %{
          "farms" => %{
            "created_at" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "deleted_on" => %{
              "auto_increment" => false,
              "default" => "NULL",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT NULL",
              "type" => "datetime",
              "unique" => false
            },
            "description" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "sql" => "TEXT DEFAULT NULL",
              "type" => "text",
              "unique" => false
            },
            "height" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            },
            "id" => %{
              "auto_increment" => true,
              "default" => nil,
              "is_primary_key" => true,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
              "type" => "integer"
            },
            "is_deleted" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => true,
              "size" => 1,
              "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
              "type" => "boolean",
              "unique" => false
            },
            "last_update_on" => %{
              "auto_increment" => false,
              "default" => "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "location" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "size" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "width" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            }
          }
        },
        "track_id" => "farmers_app"
      },
      "root_cmd_node_list" => [
        add: [
          register_farms: [
            farms: %{
              "is_list" => true,
              "sql" => [
                "INSERT INTO `farms`(height, location, name, width) VALUES(400, 'jinja', 'general animal farm', 400)",
                "INSERT INTO `farms`(description, height, location, name, size, width) VALUES('\n      we can alwyas grow so much food that no one in the world will ever starv again, are my right\n      and every human in the whole world wide web is speciall\n    ', 100, 'kampala', 'bananas farm', '50 x 100', 50)"
              ]
            }
          ]
        ]
      ],
      "root_cmd_sqls_results" => %{
        "errors" => %{},
        "has_errors" => false,
        "results" => [
          add: [
            register_farms: [
              farms: %{
                "results" => [
                  ok: %MyXQL.Result{
                    columns: nil,
                    connection_id: :dao@removed,
                    last_insert_id: 1,
                    num_rows: 1,
                    num_warnings: 0,
                    rows: nil
                  },
                  ok: %MyXQL.Result{
                    columns: nil,
                    connection_id: :dao@removed,
                    last_insert_id: 2,
                    num_rows: 1,
                    num_warnings: 0,
                    rows: nil
                  }
                ],
                "sql" => [
                  "INSERT INTO `farms`(height, location, name, width) VALUES(400, 'jinja', 'general animal farm', 400)",
                  "INSERT INTO `farms`(description, height, location, name, size, width) VALUES('\n      we can alwyas grow so much food that no one in the world will ever starv again, are my right\n      and every human in the whole world wide web is speciall\n    ', 100, 'kampala', 'bananas farm', '50 x 100', 50)"
                ]
              }
            ]
          ]
        ]
      }
    }
  end

  def get_test3_expected_results() do
    %{
      "auto_schema_changes_results" => %{
        "errors" => %{},
        "has_errors" => false,
        "index_ref_results" => %{
          0 => %{
            "results" =>
              {:ok,
               %MyXQL.Result{
                 columns: nil,
                 connection_id: :dao@removed,
                 last_insert_id: 0,
                 num_rows: 0,
                 num_warnings: 0,
                 rows: nil
               }},
            "sql" => "ALTER TABLE `farms` ADD status VARCHAR(30) DEFAULT 'active'"
          }
        },
        "next_index" => 1
      },
      "connections" => %{"agric" => :any},
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => ["ALTER TABLE `farms` ADD status VARCHAR(30) DEFAULT 'active'"],
        "database_name" => "agric",
        "database_type" => "mysql",
        "remove_sql_results_connection_ids" => true,
        "reset_db" => false,
        "schema" => %{
          "farms" => %{
            "created_at" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "deleted_on" => %{
              "auto_increment" => false,
              "default" => "NULL",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT NULL",
              "type" => "datetime",
              "unique" => false
            },
            "description" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "sql" => "TEXT DEFAULT NULL",
              "type" => "text",
              "unique" => false
            },
            "height" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            },
            "id" => %{
              "auto_increment" => true,
              "default" => nil,
              "is_primary_key" => true,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
              "type" => "integer"
            },
            "is_deleted" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => true,
              "size" => 1,
              "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
              "type" => "boolean",
              "unique" => false
            },
            "last_update_on" => %{
              "auto_increment" => false,
              "default" => "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "is_primary_key" => false,
              "required" => "",
              "size" => 60,
              "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
              "type" => "datetime",
              "unique" => false
            },
            "location" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "size" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "status" => %{
              "auto_increment" => false,
              "default" => "active",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30) DEFAULT 'active'",
              "type" => "string",
              "unique" => false
            },
            "width" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            }
          }
        },
        "track_id" => "farmers_app"
      },
      "root_cmd_node_list" => [
        get: [
          list_of_farms: [
            farms: %{
              "is_list" => true,
              "sql" =>
                "SELECT `farms`.`description`, `farms`.`height`, `farms`.`location`, `farms`.`name`, `farms`.`size`, `farms`.`status`, `farms`.`width` FROM `farms` WHERE `farms`.`is_deleted` = 0"
            }
          ]
        ]
      ],
      "root_cmd_sqls_results" => %{
        "errors" => %{},
        "has_errors" => false,
        "results" => [
          get: [
            list_of_farms: [
              farms: %{
                "results" =>
                  {:ok,
                   %MyXQL.Result{
                     columns: [
                       "description",
                       "height",
                       "location",
                       "name",
                       "size",
                       "status",
                       "width"
                     ],
                     connection_id: :dao@removed,
                     last_insert_id: nil,
                     num_rows: 2,
                     num_warnings: 0,
                     rows: [
                       [nil, 400, "jinja", "general animal farm", nil, "active", 400],
                       [
                         "\n      we can alwyas grow so much food that no one in the world will ever starv again, are my right\n      and every human in the whole world wide web is speciall\n    ",
                         100,
                         "kampala",
                         "bananas farm",
                         "50 x 100",
                         "active",
                         50
                       ]
                     ]
                   }},
                "sql" =>
                  "SELECT `farms`.`description`, `farms`.`height`, `farms`.`location`, `farms`.`name`, `farms`.`size`, `farms`.`status`, `farms`.`width` FROM `farms` WHERE `farms`.`is_deleted` = 0"
              }
            ]
          ]
        ]
      }
    }
  end
end
