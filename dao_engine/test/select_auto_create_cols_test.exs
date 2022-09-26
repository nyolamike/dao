defmodule SelectAutoCreateColsTest do
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

  test "auto create columns during selection   ", %{"context" => context} do
    query = [
      get: [
        employees_list: [
          employees: %{
            "use_default_pk" => false,
            "columns" => %{
              "emp_id" => "pk",
              "first_name" => %{
                "type" => "string",
                "size" => 50,
                "required" => true
              },
              "last_name" => "string",
              "status" => %{
                "type" => "string",
                "size" => 10,
                "default" => "pending"
              }
            }
          }
        ]
      ]
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
          "CREATE TABLE `employees` (emp_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, first_name VARCHAR(50) NOT NULL, last_name VARCHAR(30), status VARCHAR(10) DEFAULT 'pending', created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "employees" => %{
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
            "emp_id" => %{
              "auto_increment" => true,
              "default" => nil,
              "is_primary_key" => true,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
              "type" => "integer"
            },
            "first_name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => true,
              "size" => 50,
              "sql" => "VARCHAR(50) NOT NULL",
              "type" => "string",
              "unique" => false
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
            "last_name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
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
            "status" => %{
              "auto_increment" => false,
              "default" => "pending",
              "is_primary_key" => false,
              "required" => false,
              "size" => 10,
              "sql" => "VARCHAR(10) DEFAULT 'pending'",
              "type" => "string",
              "unique" => false
            }
          }
        }
      },
      "root_cmd_node_list" => [
        get: [
          employees_list: [
            employees: %{
              "is_list" => true,
              "sql" =>
                "SELECT `employees`.`emp_id`, `employees`.`first_name`, `employees`.`last_name`, `employees`.`status` FROM `employees` WHERE `employees`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end
end
