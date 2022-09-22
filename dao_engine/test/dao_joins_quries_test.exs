defmodule DaoJoinsQuriesTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "1. Find all the branches and the names of their managers trying with non existing tables 3:04:40 / 4:20:38 Join Queries " do
    context = get_company_context()

    query = [
      get: [
        branches_with_managers: [
          employee: %{
            "emp_id" => "int",
            "first_name" => "string",
            "banana" => %{
              "branch_name" => "str",
              "bongo" => %{
                "bongo_name" => "str",
                "katweka" => %{
                  "makida" => 34
                }
              }
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => ["CREATE TABLE `company_db.katwekas` (makida INT(30))",
         "CREATE TABLE `company_db.bongos` (bongo_name VARCHAR(30))",
         "CREATE TABLE `company_db.bananas` (branch_name VARCHAR(30))",
         "ALTER TABLE `company_db.katwekas` ADD INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
         "ALTER TABLE `company_db.bongos` ADD katweka_id INT(30) NOT NULL, ADD FOREIGN KEY(katweka_id) REFERENCES katwekas(id) ON DELETE SET CASCADE",
         "ALTER TABLE `company_db.bongos` ADD INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
         "ALTER TABLE `company_db.bananas` ADD bongo_id INT(30) NOT NULL, ADD FOREIGN KEY(bongo_id) REFERENCES bongos(id) ON DELETE SET CASCADE",
         "ALTER TABLE `company_db.bananas` ADD INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
         "ALTER TABLE `company_db.employees` ADD banana_id INT(30) NOT NULL, ADD FOREIGN KEY(banana_id) REFERENCES bananas(id) ON DELETE SET CASCADE"],
        "dao@timestamps" => false,
        "dao@use_default_pk" => false,
        "database_name" => "company_db",
        "database_type" => "mysql",
        "schema" => %{
          "bananas" => %{
            "bongo_id" => %{
              "auto_increment" => false,
              "default" => nil,
              "fk" => "bongos",
              "is_foreign_key" => true,
              "is_primary_key" => false,
              "on" => "id",
              "on_delete" => "cascade",
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) NOT NULL",
              "type" => "integer"
            },
            "branch_name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
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
            }
          },
          "bongos" => %{
            "bongo_name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
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
            "katweka_id" => %{
              "auto_increment" => false,
              "default" => nil,
              "fk" => "katwekas",
              "is_foreign_key" => true,
              "is_primary_key" => false,
              "on" => "id",
              "on_delete" => "cascade",
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) NOT NULL",
              "type" => "integer"
            }
          },
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
            "banana_id" => %{
              "auto_increment" => false,
              "default" => nil,
              "fk" => "bananas",
              "is_foreign_key" => true,
              "is_primary_key" => false,
              "on" => "id",
              "on_delete" => "cascade",
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) NOT NULL",
              "type" => "integer"
            },
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
          "katwekas" => %{
            "id" => %{
              "auto_increment" => true,
              "default" => nil,
              "is_primary_key" => true,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
              "type" => "integer"
            },
            "makida" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
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
      },
      "root_cmd_node_list" => [
        get: [
          branches_with_managers: [
            employee: %{
              "is_list" => false,
              "sql" => "SELECT `company_db.bongos.bongo_name`, `company_db.katwekas.makida`, `company_db.bananas.branch_name`, `company_db.employees.emp_id`, `company_db.employees.first_name` FROM `company_db.employees` LEFT JOIN `company_db.katwekas` ON `company_db.bongos.katweka_id` = `company_db.katwekas.id` LEFT JOIN `company_db.bongos` ON `company_db.bananas.bongo_id` = `company_db.bongos.id` LEFT JOIN `company_db.bananas` ON `company_db.employees.banana_id` = `company_db.bananas.id` WHERE is_deleted = 0"
            }
          ]
        ]
      ]
    }
    assert expected_results == results
  end

  test "1.1 Find all the branches and the names of their managers automatically picking linking columns 3:04:40 / 4:20:38 Join Queries " do
    context = get_company_context()

    query = [
      get: [
        branches_with_managers: [
          employee: %{
            "emp_id" => "int",
            "first_name" => "string",
            "branch" => %{
              "branch_name" => "str"
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
        branches_with_managers: [
          employee: %{
            "is_list" => false,
            "sql" => "SELECT `company_db.branches.branch_name`, `company_db.employees.emp_id`, `company_db.employees.first_name` FROM `company_db.employees` LEFT JOIN `company_db.branches` ON `company_db.employees.branch_id` = `company_db.branches.branch_id` WHERE is_deleted = 0"
          }
        ]
      ]
    ]
    assert expected_results == cmd_results
  end

  test "1.2 Find all the branches and the names of their managers specifying linking cols 3:04:40 / 4:20:38 Join Queries " do
    context = get_company_context()

    query = [
      get: [
        branches_with_managers: [
          employees: %{
            "emp_id" => "int",
            "first_name" => "string",
            "branch" => %{
              "branch_name" => "str",
              "dao@link" => {"emp_id", "mgr_id"},
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
        branches_with_managers: [
          employees: %{
            "is_list" => true,
            "sql" => "SELECT `company_db.branches.branch_name`, `company_db.employees.emp_id`, `company_db.employees.first_name` FROM `company_db.employees` LEFT JOIN `company_db.branches` ON `company_db.employees.emp_id` = `company_db.branches.mgr_id` WHERE is_deleted = 0"
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
