defmodule DaoUnionQueriesTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "1. Find a list of employee and branch names 2:54:30 / 4:20:38 Union Queries " do
    context = get_company_context()

    query = [
      get: [
        names_in_our_db: [
          dao@combine: [
            employee: %{
              "first_name" => %{
                "as" => "Company_Names"
              }
            },
            branch: ["branch_name"],
            client: ["client_name"]
          ]
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        names_in_our_db: [
          dao@combine: %{
            "is_list" => true,
            "sql" =>
              "SELECT `company_db.employees.first_name` AS Company_Names FROM `company_db.employees` WHERE is_deleted = 0 UNION SELECT `company_db.branches.branch_name` FROM `company_db.branches` WHERE is_deleted = 0 UNION SELECT `company_db.clients.client_name` FROM `company_db.clients` WHERE is_deleted = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "2. Find a list of all clients and branch suppliers' names 2:58:17 / 4:20:38 Union Queries " do
    context = get_company_context()

    query = [
      get: [
        names_in_our_db: [
          dao@combine: [
            client: %{
              "client_name" => true,
              "branch_id" => true
            },
            branch_supplier: ["supplier_name", "branch_id"]
          ]
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        names_in_our_db: [
          dao@combine: %{
            "is_list" => true,
            "sql" =>
              "SELECT `company_db.clients.branch_id`, `company_db.clients.client_name` FROM `company_db.clients` WHERE is_deleted = 0 UNION SELECT `company_db.branch_suppliers.supplier_name`, `company_db.branch_suppliers.branch_id` FROM `company_db.branch_suppliers` WHERE is_deleted = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "3. Find a list of all money spent or earned by the company 3:00:38 / 4:20:38 Union Queries " do
    context = get_company_context()

    query = [
      get: [
        all_money: [
          dao@combine: [
            employee: ["salary"],
            works_with: ["total_sales"]
          ]
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        all_money: [
          dao@combine: %{
            "is_list" => true,
            "sql" => "SELECT `company_db.employees.salary` FROM `company_db.employees` WHERE is_deleted = 0 UNION SELECT `company_db.works_withs.total_sales` FROM `company_db.works_withs` WHERE is_deleted = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
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
