defmodule DaoNestedQueriesTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "1. Find the names of all employees who have sold over 30,000 to a single client 3:12:43 / 4:20:38 Nested Queries " do
    context = get_company_context()

    query = [
      get: [
        high_performers: [
          employees: %{
            "first_name" => "str",
            "last_name" => "str",
            "dao@where" => {
              "emp_id",
              "is in",
              [
                works_withs: %{
                  "emp_id" => true,
                  "dao@where" => {
                    "total_sales",
                    ">",
                    30_000
                  }
                }
              ]
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results
    assert results_context == context

    expected_results = [
      get: [
        high_performers: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT `employees`.`first_name`, `employees`.`last_name` FROM `employees` WHERE (`employees`.`emp_id` IN (SELECT `works_withs`.`emp_id` FROM `works_withs` WHERE (`works_withs`.`total_sales` > 30000) AND `works_withs`.`is_deleted` = 0)) AND `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert expected_results == cmd_results
  end

  test "2. Find all clients who where handled by the branch tha Micheal Scott manages, Assume you know Micheals's ID 3:17:11 / 4:20:38 Nested Queries " do
    context = get_company_context()

    query = [
      get: [
        managed_clients: [
          clients: %{
            "client_name" => "str",
            "dao@where" => {
              "branch_id",
              "=",
              [
                branch: %{
                  "branch_id" => "int",
                  "dao@where" => {
                    "mgr_id",
                    "=",
                    102
                  },
                  "dao@size" => 1
                }
              ]
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results
    assert results_context == context

    expected_results = [
      get: [
        managed_clients: [
          clients: %{
            "is_list" => true,
            "sql" =>
              "SELECT `clients`.`client_name` FROM `clients` WHERE (`clients`.`branch_id` = (SELECT `branches`.`branch_id` FROM `branches` WHERE (`branches`.`mgr_id` = 102) AND `branches`.`is_deleted` = 0 LIMIT 1)) AND `clients`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert expected_results == cmd_results
  end

  defp get_company_context() do
    %{
      "auto_alter_db" => true,
      "auto_schema_changes" => [],
      "dao@timestamps" => false,
      "dao@use_default_pk" => false,
      "database_name" => "company_db",
      "database_type" => "mysql",
      "schema" => %{
        "branch_suppliers" => %{
          "branch_id" => %{
            "auto_increment" => false,
            "default" => nil,
            "fk" => "branches",
            "is_foreign_key" => true,
            "is_primary_key" => false,
            "on" => "branch_id",
            "on_delete" => "cascade",
            "required" => false,
            "size" => 30,
            "sql" => "INT(30) NOT NULL",
            "type" => "integer"
          },
          "supplier_name" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "sql" => "VARCHAR(40)",
            "type" => "string",
            "unique" => false
          },
          "supplier_type" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "sql" => "VARCHAR(40)",
            "type" => "string",
            "unique" => false
          }
        },
        "branches" => %{
          "branch_id" => %{
            "auto_increment" => true,
            "default" => nil,
            "is_primary_key" => true,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
            "type" => "integer"
          },
          "branch_name" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "sql" => "VARCHAR(40)",
            "type" => "string",
            "unique" => false
          },
          "mgr_id" => %{
            "auto_increment" => false,
            "default" => nil,
            "fk" => "employees",
            "is_foreign_key" => true,
            "is_primary_key" => false,
            "on" => "emp_id",
            "on_delete" => nil,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer"
          },
          "mgr_start_date" => %{
            "auto_increment" => false,
            "default" => nil,
            "is_primary_key" => false,
            "required" => "",
            "sql" => "DATE",
            "type" => "date",
            "unique" => false
          }
        },
        "clients" => %{
          "branch_id" => %{
            "auto_increment" => false,
            "default" => "",
            "fk" => "employees",
            "is_foreign_key" => true,
            "is_primary_key" => false,
            "on" => "emp_id",
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer",
            "unique" => false
          },
          "client_id" => %{
            "auto_increment" => true,
            "default" => nil,
            "is_primary_key" => true,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
            "type" => "integer"
          },
          "client_name" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 44,
            "sql" => "VARCHAR(44)",
            "type" => "string",
            "unique" => false
          }
        },
        "employees" => %{
          "birth_day" => %{
            "auto_increment" => false,
            "default" => nil,
            "is_primary_key" => false,
            "required" => "",
            "sql" => "DATE",
            "type" => "date",
            "unique" => false
          },
          "branch_id" => %{
            "auto_increment" => false,
            "default" => nil,
            "fk" => "branches",
            "is_foreign_key" => true,
            "is_primary_key" => false,
            "on" => "branch_id",
            "on_delete" => nil,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer"
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
            "required" => false,
            "size" => 40,
            "sql" => "VARCHAR(40)",
            "type" => "string",
            "unique" => false
          },
          "last_name" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "sql" => "VARCHAR(40)",
            "type" => "string",
            "unique" => false
          },
          "salary" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer",
            "unique" => false
          },
          "sex" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer",
            "unique" => false
          },
          "super_id" => %{
            "auto_increment" => false,
            "default" => nil,
            "fk" => "employees",
            "is_foreign_key" => true,
            "is_primary_key" => false,
            "on" => "emp_id",
            "on_delete" => nil,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer"
          }
        },
        "works_withs" => %{
          "client_id" => %{
            "auto_increment" => false,
            "default" => nil,
            "fk" => "clients",
            "is_foreign_key" => true,
            "is_primary_key" => false,
            "on" => "client_id",
            "on_delete" => "cascade",
            "required" => false,
            "size" => 30,
            "sql" => "INT(30) NOT NULL",
            "type" => "integer"
          },
          "emp_id" => %{
            "auto_increment" => false,
            "default" => nil,
            "fk" => "employees",
            "is_foreign_key" => true,
            "is_primary_key" => false,
            "on" => "emp_id",
            "on_delete" => "cascade",
            "required" => false,
            "size" => 30,
            "sql" => "INT(30) NOT NULL",
            "type" => "integer"
          },
          "total_sales" => %{
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
      }
    }
  end
end
