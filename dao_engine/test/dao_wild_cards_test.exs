defmodule DaoWildCardsTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "1. Find clients who are LLC 2:45:13 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        llc_clients: [
          clients: %{
            "dao@where" => {
              "client_name",
              "ends with",
              "LLC"
            }
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        llc_clients: [
          clients: %{
            "is_list" => true,
            "sql" =>
              "SELECT * FROM `clients` WHERE (`clients`.`client_name` LIKE '%LLC') AND `clients`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "2. Find any branch suppliers who are in the label business 2:48:47 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        label_suppliers: [
          branch_supplier: %{
            "dao@where" => {
              "supplier_name",
              "contains",
              " Label"
            }
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        label_suppliers: [
          branch_supplier: %{
            "is_list" => false,
            "sql" =>
              "SELECT * FROM `branch_suppliers` WHERE (`branch_suppliers`.`supplier_name` LIKE '% Label%') AND `branch_suppliers`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "3. Find any employee born in october 2:50:39 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        oct_babies: [
          employee: %{
            "dao@where" => {
              "birth_date",
              "matches",
              "____-10%"
            }
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        oct_babies: [
          employee: %{
            "is_list" => false,
            "sql" =>
              "SELECT * FROM `employees` WHERE (`employees`.`birth_date` LIKE '____-10%') AND `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "4. Find any clients who are a school 2:52:37 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        school_clients: [
          client: %{
            "dao@where" => {
              "client_name",
              "has",
              "school"
            }
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        school_clients: [
          client: %{
            "is_list" => false,
            "sql" =>
              "SELECT * FROM `clients` WHERE (`clients`.`client_name` LIKE '%school%') AND `clients`.`is_deleted` = 0"
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
