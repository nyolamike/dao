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
            "sex" => "int",
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
          "CREATE TABLE `company_db.employees` (birth_day DATE, branch_id INT(30), emp_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, first_name VARCHAR(40), last_name VARCHAR(40), sex INT(30), super_id INT(30))"
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
          "CREATE TABLE `company_db.branches` (branch_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, branch_name VARCHAR(40), mgr_id INT(30), mgr_start_date DATE, FOREIGN KEY(mgr_id) REFERENCES employees(emp_id) ON DELETE SET NULL)"
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
          "dao@skip: ALTER TABLE `company_db.employees` ADD FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL, ADD FOREIGN KEY(super_id) REFERENCES employees(emp_id) ON DELETE SET NULL"
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
                "ALTER TABLE `company_db.employees` ADD FOREIGN KEY(branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL, ADD FOREIGN KEY(super_id) REFERENCES employees(emp_id) ON DELETE SET NULL"
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

    IO.inspect(results)
  end


end
