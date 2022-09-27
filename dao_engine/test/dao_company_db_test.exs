defmodule DaoCompanyDbTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "1. creates employee table @ 2:19:26 / 4:20:38 Creating Company Database" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "company_db",
      "schema" => %{},
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      # "dao@timestamps" => false,
      "use_standard_timestamps" => false,
      # "dao@use_default_pk" => false,
      "use_default_pk" => false
    }

    query = [
      add: [
        employees_table: [
          employee: %{
            "emp_id" => "pk",
            "first_name" => "str 40",
            "last_name" => "str 40",
            "birth_day" => "date",
            "sex" => "str 1",
            "super_id" => "int",
            "branch_id" => "int"
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `employees` (birth_day DATE, branch_id INT(30), emp_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, first_name VARCHAR(40), last_name VARCHAR(40), sex VARCHAR(1), super_id INT(30))"
        ],
        "database_name" => "company_db",
        "database_type" => "mysql",
        "schema" => %{
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
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
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
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
              "unique" => false,
              "default" => "",
              "sql" => "VARCHAR(40)"
            },
            "last_name" => %{
              "auto_increment" => false,
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
              "unique" => false,
              "default" => "",
              "sql" => "VARCHAR(40)"
            },
            "sex" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 1,
              "sql" => "VARCHAR(1)",
              "type" => "string",
              "unique" => false
            },
            "super_id" => %{
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
        "use_default_pk" => false,
        "use_standard_timestamps" => false
      },
      "root_cmd_node_list" => [
        add: [employees_table: [employee: %{"is_list" => false, "sql" => ""}]]
      ]
    }

    assert expected_results == results
  end

  test "2. creates branches table 2:20:20 / 4:20:38 Creating Company Database" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "company_db",
      "schema" => %{
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
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer",
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
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
            "unique" => false
          },
          "last_name" => %{
            "auto_increment" => false,
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
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
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "dao@timestamps" => false,
      "dao@use_default_pk" => false
    }

    query = [
      add: [
        branches_table: [
          branch: %{
            "branch_id" => "pk",
            "branch_name" => %{
              "type" => "string",
              "size" => 40
            },
            "mgr_id" => %{
              "fk" => "employee",
              "on" => "emp_id",
              "on_delete" => nil
            },
            "mgr_start_date" => "date"
            # "dao@fks" => [
            #   {"mgr_id", "employee", "employee",, nil}
            # ]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `branches` (branch_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, branch_name VARCHAR(40), mgr_id INT(30), mgr_start_date DATE, FOREIGN KEY(mgr_id) REFERENCES employees(emp_id) ON DELETE SET NULL)"
        ],
        "dao@timestamps" => false,
        "dao@use_default_pk" => false,
        "database_name" => "company_db",
        "database_type" => "mysql",
        "schema" => %{
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
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "on" => "emp_id",
              "on_delete" => nil
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
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
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
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
              "unique" => false
            },
            "last_name" => %{
              "auto_increment" => false,
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
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
        add: [branches_table: [branch: %{"is_list" => false, "sql" => ""}]]
      ]
    }

    assert expected_results == results
  end

  test "3. creates foreign keys on employee table 2:22:00 / 4:20:38 Creating Company Database" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "company_db",
      "schema" => %{
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
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer",
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
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
            "unique" => false
          },
          "last_name" => %{
            "auto_increment" => false,
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
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
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30)",
            "type" => "integer",
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
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "dao@timestamps" => false,
      "dao@use_default_pk" => false
    }

    # nyd: this has to change the schema table configs for the relevant columns to show that they are foreign keys
    # nyd: the query below works here when the table and columns already exist, what is they dont exist
    query = [
      add: [
        employee_foreign_keys: [
          employee: %{
            "dao@fks" => %{
              "branch_id" => %{
                "fk" => "branch",
                "on" => "branch_id",
                "on_delete" => nil
              },
              "super_id" => %{
                "fk" => "employee",
                "on" => "emp_id",
                "on_delete" => nil
              }
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "dao@skip: ALTER TABLE `employees` ADD FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL, ADD FOREIGN KEY(super_id) REFERENCES employees(emp_id) ON DELETE SET NULL"
        ],
        "dao@timestamps" => false,
        "dao@use_default_pk" => false,
        "database_name" => "company_db",
        "database_type" => "mysql",
        "schema" => %{
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
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
              "unique" => false
            },
            "last_name" => %{
              "auto_increment" => false,
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
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
          }
        }
      },
      "root_cmd_node_list" => [
        add: [
          employee_foreign_keys: [
            employee: %{
              "is_list" => false,
              "sql" =>
                "ALTER TABLE `employees` ADD FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL, ADD FOREIGN KEY(super_id) REFERENCES employees(emp_id) ON DELETE SET NULL"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "4. creates branches table 2:20:20 / 4:20:38 Creating Company Database" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "company_db",
      "schema" => %{
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
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
            "unique" => false
          },
          "last_name" => %{
            "auto_increment" => false,
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
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
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "dao@timestamps" => false,
      "dao@use_default_pk" => false
    }

    query = [
      add: [
        client_table: [
          client: %{
            "client_id" => "pk",
            "client_name" => "str 44",
            "branch_id" => "int",
            "dao@fks" => %{
              "branch_id" => %{
                "fk" => "employee",
                "on" => "emp_id",
                "on_delete" => nil
              }
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `clients` (branch_id INT(30), client_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, client_name VARCHAR(44), ADD FOREIGN KEY(branch_id) REFERENCES employees(emp_id) ON DELETE SET NULL)"
        ],
        "dao@timestamps" => false,
        "dao@use_default_pk" => false,
        "database_name" => "company_db",
        "database_type" => "mysql",
        "schema" => %{
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
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false,
              "fk" => "employees",
              "is_foreign_key" => true,
              "on" => "emp_id"
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
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
              "unique" => false
            },
            "last_name" => %{
              "auto_increment" => false,
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
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
          }
        }
      },
      "root_cmd_node_list" => [
        add: [client_table: [client: %{"is_list" => false, "sql" => ""}]]
      ]
    }

    assert expected_results == results
  end

  test "5. creates works_with table composite primary keys 2:23:24 / 4:20:38 Creating Company Database" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "company_db",
      "schema" => %{
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
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
            "unique" => false
          },
          "last_name" => %{
            "auto_increment" => false,
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
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
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "dao@timestamps" => false,
      "dao@use_default_pk" => false
    }

    # nyd: the only issue with this query is that "dao@pks" wont update the columns in the dao schema context to show that they ae primary keys
    query = [
      add: [
        works_with_table: [
          works_with: %{
            "emp_id" => %{
              "fk" => "employee",
              "on" => "emp_id",
              "on_delete" => "cascade"
            },
            "client_id" => "fk",
            "total_sales" => "int",
            "dao@pks" => ["emp_id", "client_id"]
            # "emp_id" => "pk",
            # "client_id" => "pk",
            # "total_sales" => "int",
            # "dao@fks" => %{
            #   "client_id" => %{
            #     "fk" => "client",
            #     "on" => "client_id",
            #     "on_delete" => "cascade"
            #   },
            #   "emp_id" => %{
            #     "fk" => "employee",
            #     "on" => "emp_id",
            #     "on_delete" => "cascade"
            #   }
            # }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `works_withs` (client_id INT(30) NOT NULL, emp_id INT(30) NOT NULL, total_sales INT(30), PRIMARY KEY(emp_id, client_id), FOREIGN KEY(client_id) REFERENCES clients(client_id) ON DELETE CASCADE,FOREIGN KEY(emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE)"
        ],
        "dao@timestamps" => false,
        "dao@use_default_pk" => false,
        "database_name" => "company_db",
        "database_type" => "mysql",
        "schema" => %{
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
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
              "unique" => false
            },
            "last_name" => %{
              "auto_increment" => false,
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
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
      },
      "root_cmd_node_list" => [
        add: [works_with_table: [works_with: %{"is_list" => false, "sql" => ""}]]
      ]
    }

    assert expected_results == results
  end

  test "6. creates branch_supplier table composite primary keys 2:24:22 / 4:20:38 Creating Company Database" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "company_db",
      "schema" => %{
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
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
            "unique" => false
          },
          "last_name" => %{
            "auto_increment" => false,
            "is_primary_key" => false,
            "required" => false,
            "size" => 40,
            "type" => "string",
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
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "dao@timestamps" => false,
      "dao@use_default_pk" => false
    }

    # nyd: the only issue with this query is that "dao@pks" wont update the columns in the dao schema context to show that they ae primary keys
    query = [
      add: [
        branch_supplier_table: [
          branch_supplier: %{
            "branch_id" => "fk",
            "supplier_name" => "str 40",
            "supplier_type" => "str 40",
            "dao@pks" => ["branch_id", "supplier_name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `branch_suppliers` (branch_id INT(30) NOT NULL, supplier_name VARCHAR(40), supplier_type VARCHAR(40), PRIMARY KEY(branch_id, supplier_name), FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE)"
        ],
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
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
              "unique" => false
            },
            "last_name" => %{
              "auto_increment" => false,
              "is_primary_key" => false,
              "required" => false,
              "size" => 40,
              "type" => "string",
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
      },
      "root_cmd_node_list" => [
        add: [branch_supplier_table: [branch_supplier: %{"is_list" => false, "sql" => ""}]]
      ]
    }

    assert expected_results == results
  end

  test "7.  Company Database - Creating all database tables in one query" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "company_db",
      "schema" => %{},
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "dao@timestamps" => false,
      "dao@use_default_pk" => false
    }

    query = [
      add: [
        employees_table: [
          employee: %{
            "emp_id" => "pk",
            "first_name" => "str 40",
            "last_name" => "str 40",
            "birth_day" => "date",
            "sex" => "str 1",
            "super_id" => "int",
            "branch_id" => "int"
          }
        ],
        branches_table: [
          branch: %{
            "branch_id" => "pk",
            "branch_name" => %{
              "type" => "string",
              "size" => 40
            },
            "mgr_id" => %{
              "fk" => "employee",
              "on" => "emp_id",
              "on_delete" => nil
            },
            "mgr_start_date" => "date"
          }
        ],
        employee_foreign_keys: [
          employee: %{
            "dao@fks" => %{
              "branch_id" => %{
                "fk" => "branch",
                "on" => "branch_id",
                "on_delete" => nil
              },
              "super_id" => %{
                "fk" => "employee",
                "on" => "emp_id",
                "on_delete" => nil
              }
            }
          }
        ],
        client_table: [
          client: %{
            "client_id" => "pk",
            "client_name" => "str 44",
            "branch_id" => "int",
            "dao@fks" => %{
              "branch_id" => %{
                "fk" => "employee",
                "on" => "emp_id",
                "on_delete" => nil
              }
            }
          }
        ],
        works_with_table: [
          works_with: %{
            "emp_id" => %{
              "fk" => "employee",
              "on" => "emp_id",
              "on_delete" => "cascade"
            },
            "client_id" => "fk",
            "total_sales" => "int",
            "dao@pks" => ["emp_id", "client_id"]
          }
        ],
        branch_supplier_table: [
          branch_supplier: %{
            "branch_id" => "fk",
            "supplier_name" => "str 40",
            "supplier_type" => "str 40",
            "dao@pks" => ["branch_id", "supplier_name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `employees` (birth_day DATE, branch_id INT(30), emp_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, first_name VARCHAR(40), last_name VARCHAR(40), sex VARCHAR(1), super_id INT(30))",
          "CREATE TABLE `branches` (branch_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, branch_name VARCHAR(40), mgr_id INT(30), mgr_start_date DATE, FOREIGN KEY(mgr_id) REFERENCES employees(emp_id) ON DELETE SET NULL)",
          "dao@skip: ALTER TABLE `employees` ADD FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL, ADD FOREIGN KEY(super_id) REFERENCES employees(emp_id) ON DELETE SET NULL",
          "CREATE TABLE `clients` (branch_id INT(30), client_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, client_name VARCHAR(44), ADD FOREIGN KEY(branch_id) REFERENCES employees(emp_id) ON DELETE SET NULL)",
          "CREATE TABLE `works_withs` (client_id INT(30) NOT NULL, emp_id INT(30) NOT NULL, total_sales INT(30), PRIMARY KEY(emp_id, client_id), FOREIGN KEY(client_id) REFERENCES clients(client_id) ON DELETE CASCADE,FOREIGN KEY(emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE)",
          "CREATE TABLE `branch_suppliers` (branch_id INT(30) NOT NULL, supplier_name VARCHAR(40), supplier_type VARCHAR(40), PRIMARY KEY(branch_id, supplier_name), FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE)"
        ],
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
            "sex" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 1,
              "sql" => "VARCHAR(1)",
              "type" => "string",
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
      },
      "root_cmd_node_list" => [
        add: [
          branch_supplier_table: [
            branch_supplier: %{"is_list" => false, "sql" => ""}
          ],
          works_with_table: [works_with: %{"is_list" => false, "sql" => ""}],
          client_table: [client: %{"is_list" => false, "sql" => ""}],
          employee_foreign_keys: [
            employee: %{
              "is_list" => false,
              "sql" =>
                "ALTER TABLE `employees` ADD FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL, ADD FOREIGN KEY(super_id) REFERENCES employees(emp_id) ON DELETE SET NULL"
            }
          ],
          branches_table: [branch: %{"is_list" => false, "sql" => ""}],
          employees_table: [employee: %{"is_list" => false, "sql" => ""}]
        ]
      ]
    }

    assert expected_results == results
  end

  test "8. Inserting employee and branch entries for the corporate branch" do
    context = get_company_context()
    # nyd: please note that one has to follow the order of the column names
    # birth_day, branch_id, emp_id, first_name, last_name, salary, sex, super_id
    # branch
    # branch_id, branch_name, mgr_id, mgr_start_date
    query = [
      add: [
        corporate_employee: [
          employee: ["1967-11-17", nil, 100, "David", "Wallace", 250_000, "M", nil]
        ],
        corporate_branch: [
          branch: [1, "Corporate", 100, "2006-02-09"]
        ]
      ],
      update: [
        employee_mgr: [
          employee: %{
            "branch_id" => 1,
            "dao@where" => {"empl_id", "is equals to", 100}
          }
        ]
      ],
      add: [
        another_employee: [
          employee: ["1961-05-11", 1, 101, "Jan", "Levinson", 110_000, "F", 100]
        ]
      ]
    ]

    %{"root_cmd_node_list" => results} = Dao.execute(context, query)

    expected_results = [
      {
        :add,
        [
          another_employee: [
            employee: %{
              "is_list" => false,
              "sql" =>
                "INSERT INTO `employees` VALUES('1961-05-11', 1, 101, 'Jan', 'Levinson', 110000, 'F', 100)"
            }
          ]
        ]
      },
      {:update,
       [
         employee_mgr: [
           employee: %{
             "is_list" => false,
             "sql" =>
               "UPDATE `employees` SET branch_id = 1 WHERE  WHERE (`employees`.`empl_id` = 100) AND `employees`.`is_deleted` = 0"
           }
         ]
       ]},
      {:add,
       [
         corporate_branch: [
           branch: %{
             "is_list" => false,
             "sql" => "INSERT INTO `branches` VALUES(1, 'Corporate', 100, '2006-02-09')"
           }
         ],
         corporate_employee: [
           employee: %{
             "is_list" => false,
             "sql" =>
               "INSERT INTO `employees` VALUES('1967-11-17', NULL, 100, 'David', 'Wallace', 250000, 'M', NULL)"
           }
         ]
       ]}
    ]

    assert expected_results == results
  end

  test "9. Inserting employee and branch entries mixed queries" do
    context = get_company_context()
    # nyd: please note that one has to follow the order of the column names
    # birth_day, branch_id, emp_id, first_name, last_name, salary, sex, super_id
    # branch
    # branch_id, branch_name, mgr_id, mgr_start_date
    query = [
      add: [
        some_employees: [
          employees: [
            ["1967-11-17", nil, 100, "David", "Wallace", 250_000, "M", nil],
            ["1964-03-15", nil, 102, "Micheal", "Scott", 250_000, "M", nil]
          ]
        ],
        branches: [
          branch: [1, "Corporate", 100, "2006-02-09"],
          branch: [2, "Scranton", 102, "1992-04-06"]
        ]
      ],
      update: [
        employee_mgr: [
          employees: [
            %{
              "branch_id" => 1,
              "dao@where" => {"empl_id", "is equals to", 100}
            },
            %{
              "branch_id" => 2,
              "dao@where" => {"empl_id", "is equals to", 102}
            }
          ]
        ]
      ],
      add: [
        more_employees: [
          employee: [
            ["1961-05-11", 1, 101, "Jan", "Levinson", 110_000, "F", 100],
            ["1971-06-25", 1, 103, "Angela", "Martin", 110_000, "F", 100],
            ["1980-02-05", 1, 104, "Kelly", "Kapoor", 110_000, "F", 100],
            ["1958-02-19", 1, 105, "Stanley", "Hudson", 110_000, "F", 100]
          ]
        ]
      ]
    ]

    %{"root_cmd_node_list" => results} = Dao.execute(context, query)

    expected_results = [
      add: [
        more_employees: [
          employee: %{
            "is_list" => false,
            "sql" =>
              "INSERT INTO `employees` VALUES('1961-05-11', 1, 101, 'Jan', 'Levinson', 110000, 'F', 100), ('1971-06-25', 1, 103, 'Angela', 'Martin', 110000, 'F', 100), ('1980-02-05', 1, 104, 'Kelly', 'Kapoor', 110000, 'F', 100), ('1958-02-19', 1, 105, 'Stanley', 'Hudson', 110000, 'F', 100)"
          }
        ]
      ],
      update: [
        employee_mgr: [
          employees: %{
            "is_list" => true,
            "sql" => [
              "UPDATE `employees` SET branch_id = 1 WHERE  WHERE (`employees`.`empl_id` = 100) AND `employees`.`is_deleted` = 0",
              "UPDATE `employees` SET branch_id = 2 WHERE  WHERE (`employees`.`empl_id` = 102) AND `employees`.`is_deleted` = 0"
            ]
          }
        ]
      ],
      add: [
        branches: [
          branch: %{
            "is_list" => false,
            "sql" => "INSERT INTO `branches` VALUES(2, 'Scranton', 102, '1992-04-06')"
          }
        ],
        some_employees: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "INSERT INTO `employees` VALUES('1967-11-17', NULL, 100, 'David', 'Wallace', 250000, 'M', NULL), ('1964-03-15', NULL, 102, 'Micheal', 'Scott', 250000, 'M', NULL)"
          }
        ]
      ]
    ]

    assert expected_results == results
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
