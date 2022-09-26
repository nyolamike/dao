defmodule GenSqlGetFixtureTest do
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

  test "generate sql for a get fixture ", %{"context" => context} do
    shop_fixtures_kwl_node = [
      # a fixture
      shops: %{}
    ]

    aircraft_fixtures_kwl_node = [
      aircraft: %{
        "is_list" => false
      }
    ]

    query = [
      get: [
        list_of_shops: shop_fixtures_kwl_node
      ],
      get: [
        biggest_aircraft: aircraft_fixtures_kwl_node
      ]
    ]

    results = Dao.gen_sql_for_get_fixture(context, query, shop_fixtures_kwl_node)

    expected_results = get_expected_results()

    results_craft = Dao.gen_sql_for_get_fixture(context, query, aircraft_fixtures_kwl_node)

    expected_results_craft = get_expected_results_aircraft()

    assert expected_results == results
    assert expected_results_craft == results_craft
  end

  defp get_expected_results() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `shops` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
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
      "fixture_list" => [
        shops: %{
          "is_list" => true,
          "sql" => "SELECT * FROM `shops` WHERE `shops`.`is_deleted` = 0"
        }
      ]
    }
  end

  defp get_expected_results_aircraft() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
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
          }
        }
      },
      "fixture_list" => [
        aircraft: %{
          "is_list" => false,
          "sql" => "SELECT * FROM `aircraft` WHERE `aircraft`.`is_deleted` = 0"
        }
      ]
    }
  end
end
