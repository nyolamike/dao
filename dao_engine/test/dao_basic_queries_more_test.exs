defmodule DaoBasicQueriesMoreTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "1. Having multiple tasks under the named query node" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string"
        },
        "funs" => %{
          "name" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "track_id" => "benja"
    }

    query = [
      get: [
        funny_students: [
          students: %{},
          funs: %{}
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "funs" => %{"name" => "string"},
          "students" => %{"name" => "string"}
        },
        "track_id" => "benja"
      },
      "root_cmd_node_list" => [
        get: [
          funny_students: [
            funs: %{
              "is_list" => true,
              "sql" => "SELECT * FROM `funs` WHERE `funs`.`is_deleted` = 0"
            },
            students: %{
              "is_list" => true,
              "sql" => "SELECT * FROM `students` WHERE `students`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "2. Find all employees 2:31:01 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        all_employees: [
          employees: %{}
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        all_employees: [
          employees: %{
            "is_list" => true,
            "sql" => "SELECT * FROM `employees` WHERE `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "3. Find all clients 2:31:30 / 4:20:38  More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        all_clients: [
          clients: %{}
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        all_clients: [
          clients: %{
            "is_list" => true,
            "sql" => "SELECT * FROM `clients` WHERE `clients`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "4. Find all employees ordered by salary 2:31:49 / 4:20:38 More Basic Queries   " do
    context = get_company_context() |> Map.put("track_id", "bada")

    query = [
      get: [
        all_employees: [
          employees: %{
            "dao@order_by" => ["salary"]
          }
        ],
        all_employees_descending: [
          employees: %{
            "dao@order_by_desc" => ["salary"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        {
          :all_employees_descending,
          [
            employees: %{
              "is_list" => true,
              "sql" =>
                "SELECT * FROM `employees` WHERE `employees`.`is_deleted` = 0 ORDER BY salary DESC"
            }
          ]
        },
        {:all_employees,
         [
           employees: %{
             "is_list" => true,
             "sql" =>
               "SELECT * FROM `employees` WHERE `employees`.`is_deleted` = 0 ORDER BY salary"
           }
         ]}
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "5. Find all employees ordered by sex then name 2:32:48 / 4:20:38 More Basic Queries   " do
    context = get_company_context()

    query = [
      get: [
        ordered_employees: [
          employees: %{
            "dao@order_by" => ["sex", "first_name", "last_name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        ordered_employees: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT * FROM `employees` WHERE `employees`.`is_deleted` = 0 ORDER BY sex, first_name, last_name"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "6. Find the first five employees in the table 2:33:43 / 4:20:38 More Basic Queries   " do
    context = get_company_context()

    query = [
      get: [
        first_5_employees: [
          employees: %{
            "dao@take" => 5
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        first_5_employees: [
          employees: %{
            "is_list" => true,
            "sql" => "SELECT * FROM `employees` WHERE `employees`.`is_deleted` = 0 LIMIT 5"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "7. Find the first and last names of all  2:34:04 / 4:20:38 More Basic Queries   " do
    context = get_company_context()

    query = [
      get: [
        names: [
          employees: ["first_name", "last_name"]
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        names: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT `employees`.`first_name`, `employees`.`last_name` FROM `employees` WHERE `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "8. Find the forename and surnames of all the mployees 2:34:29 / 4:20:38 More Basic Queries   " do
    context = get_company_context()

    query = [
      get: [
        names: [
          employees: %{
            "first_name" => %{
              "as" => "forename"
            },
            "last_name" => %{
              "as" => "surname"
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        names: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT `employees`.`first_name` AS forename, `employees`.`last_name` AS surname FROM `employees` WHERE `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "9. Find the different genders 2:35:25 / 4:20:38 More Basic Queries   " do
    context = get_company_context()

    query = [
      get: [
        gender_of_employees: [
          employees: %{
            "dao@unique" => ["sex"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        gender_of_employees: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT DISTINCT `employees`.`sex` FROM `employees` WHERE `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "10. Find the number of employees 2:36:58 / 4:20:38 More Basic Queries   " do
    context = get_company_context()

    query = [
      get: [
        num_employees: [
          employees: %{
            "dao@count" => ["emp_id"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        num_employees: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT COUNT(`employees`.`emp_id`) FROM `employees` WHERE `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "11. Find the number of employees born after 1970 2:38:23 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        old_employees: [
          employees: %{
            "dao@count" => ["emp_id"],
            "dao@where" => {
              {"sex", "=", "F"},
              "&&",
              {"birth_date", ">=", "1971-01-01"}
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        old_employees: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT COUNT(`employees`.`emp_id`) FROM `employees` WHERE ((`employees`.`sex` = 'F') AND (`employees`.`birth_date` >= '1971-01-01')) AND `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "12. Find the average of all employee's salaries 2:39:39 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        salary_average_for_all: [
          employees: %{
            "dao@average" => ["salary"]
          }
        ],
        salary_average_for_male: [
          employees: %{
            "dao@average" => ["salary"],
            "dao@where" => {"sex", "=", "M"}
          }
        ],
        salary_average_for_female: [
          employees: %{
            "dao@average" => ["salary"],
            "dao@where" => {"sex", "=", "F"}
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        {
          :salary_average_for_female,
          [
            employees: %{
              "is_list" => true,
              "sql" =>
                "SELECT AVG(`employees`.`salary`) FROM `employees` WHERE (`employees`.`sex` = 'F') AND `employees`.`is_deleted` = 0"
            }
          ]
        },
        {:salary_average_for_male,
         [
           employees: %{
             "is_list" => true,
             "sql" =>
               "SELECT AVG(`employees`.`salary`) FROM `employees` WHERE (`employees`.`sex` = 'M') AND `employees`.`is_deleted` = 0"
           }
         ]},
        {:salary_average_for_all,
         [
           employees: %{
             "is_list" => true,
             "sql" =>
               "SELECT AVG(`employees`.`salary`) FROM `employees` WHERE `employees`.`is_deleted` = 0"
           }
         ]}
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "13. Find the sum of all employee's salaries 2:40:36 / 4:20:38 More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        salary_sum_for_all: [
          employees: %{
            "dao@sum" => ["salary"]
          }
        ],
        salary_sum_for_male: [
          employees: %{
            "dao@sum" => ["salary"],
            "dao@where" => {"sex", "=", "M"}
          }
        ],
        salary_sum_for_female: [
          employees: %{
            "dao@sum" => ["salary"],
            "dao@where" => {"sex", "=", "F"}
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        salary_sum_for_female: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT SUM(`employees`.`salary`) FROM `employees` WHERE (`employees`.`sex` = 'F') AND `employees`.`is_deleted` = 0"
          }
        ],
        salary_sum_for_male: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT SUM(`employees`.`salary`) FROM `employees` WHERE (`employees`.`sex` = 'M') AND `employees`.`is_deleted` = 0"
          }
        ],
        salary_sum_for_all: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT SUM(`employees`.`salary`) FROM `employees` WHERE `employees`.`is_deleted` = 0"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "14. Find out how many males and females there are 2:41:18 / 4:20:38 Aggreagation More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        sex_counts: [
          employees: %{
            "sex" => true,
            "dao@count" => ["sex"],
            "dao@group_by" => ["sex"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        sex_counts: [
          employees: %{
            "is_list" => true,
            "sql" =>
              "SELECT COUNT(`employees`.`sex`), `employees`.`sex` FROM `employees` WHERE `employees`.`is_deleted` = 0 GROUP BY sex"
          }
        ]
      ]
    ]

    assert context == results_context
    assert expected_cmd_results == cmd_results
  end

  test "15. Find the total sales of each sales man 2:42:59 / 4:20:38 Aggreagation More Basic Queries " do
    context = get_company_context()

    query = [
      get: [
        sales_per_employee: [
          works_with: %{
            "emp_id" => true,
            "dao@total" => ["total_sales"],
            "dao@group_by" => ["emp_id"]
          }
        ],
        clients_expenditure: [
          works_with: %{
            "client_id" => true,
            "dao@total" => ["total_sales"],
            "dao@group_by" => ["client_id"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    %{"context" => results_context, "root_cmd_node_list" => cmd_results} = results

    expected_cmd_results = [
      get: [
        clients_expenditure: [
          works_with: %{
            "is_list" => false,
            "sql" =>
              "SELECT SUM(`works_withs`.`total_sales`) FROM `works_withs` WHERE `works_withs`.`is_deleted` = 0 GROUP BY client_id"
          }
        ],
        sales_per_employee: [
          works_with: %{
            "is_list" => false,
            "sql" =>
              "SELECT SUM(`works_withs`.`total_sales`), `works_withs`.`emp_id` FROM `works_withs` WHERE `works_withs`.`is_deleted` = 0 GROUP BY emp_id"
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
