defmodule GetQueryTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  setup do
    %{
      "context" => %{
        "database_type" => "mysql",
        "database_name" => "grocerify",
        "schema" => %{},
        "auto_schema_changes" => [],
        "auto_alter_db" => true
      }
    }
  end

  test "executes get query ", %{"context" => context} do
    shop_get_cmd_node = [
      list_of_shops: [
        # a fixture
        shops: %{}
      ]
    ]

    aircraft_get_cmd_node = [
      biggest_aircraft: [
        aircraft: %{
          "is_list" => false
        }
      ]
    ]

    query = [
      get: shop_get_cmd_node,
      get: aircraft_get_cmd_node
    ]

    results = Dao.execute(context, query)

    expected_results = get_expected_results()

    assert expected_results == results
  end

  defp get_expected_results() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `shops` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)",
          "CREATE TABLE `aircraft` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "aircraft" => %{
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
            }
          },
          "shops" => %{
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
            }
          }
        }
      },
      "root_cmd_node_list" => [
        get: [
          biggest_aircraft: [
            aircraft: %{
              "is_list" => false,
              "sql" =>
                "SELECT * FROM `aircraft` WHERE `aircraft`.`is_deleted` = 0"
            }
          ]
        ],
        get: [
          list_of_shops: [
            shops: %{
              "is_list" => true,
              "sql" => "SELECT * FROM `shops` WHERE `shops`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end
end
