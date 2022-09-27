defmodule DaoStudentGenUpdateSqlTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  setup do
    %{
      "context" => %{
        "database_type" => "mysql",
        "database_name" => "grocerify",
        "schema" => %{
          "students" => %{
            "name" => "string"
          }
        },
        "auto_schema_changes" => [],
        "auto_alter_db" => true
      }
    }
  end

  test "generate sql to update a student where table does not exist", %{"context" => context} do
    # delete the students table from schema
    # UPDATE student SET major = 'Bio' WHERE major = 'Biology'
    context = %{context | "schema" => Map.delete(context["schema"], "students")}

    query = [
      update: [
        students_major: [
          students: %{
            "major" => "Bio",
            "dao@where" => {"major", "equals", "Biology"}
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = get_expected_results()
    assert expected_results == results
  end

  test "generate sql to update a student where column does not exist", %{"context" => context} do
    # UPDATE student SET major = 'Bio' WHERE major = 'Biology'

    query = [
      update: [
        students_major: [
          students: %{
            "name" => "Muwanguzi",
            "major" => "Bio",
            "gpa" => 23.5,
            "dao@where" => {"major", "equals", "Biology"}
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results_missing_cols = get_expected_results_missing_cols()
    assert expected_results_missing_cols == results
  end

  test "generate sql to update a student where multiple conditions" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "gpa" => "decimal",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      update: [
        students_major: [
          students: %{
            "major" => "Bio",
            "dao@where" => {
              {"major", "equals", "Biology"},
              "and",
              {"gpa", "equals", 50}
            }
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_single_and_condition = get_expected_results_single_and_condition()
    assert expected_single_and_condition == results
  end

  test "generate sql to update a student where multiple conditions with an or" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "gpa" => "decimal",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      update: [
        students_major: [
          students: %{
            "major" => "Bio",
            "dao@where" => {
              {
                {"major", "equals", "Biology"},
                "and",
                {"gpa", "equals", 50}
              },
              "or",
              {"major", "equals", "Mathematics"}
            }
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = get_epected_results_or()
    assert expected_results == results
  end

  test "generate sql to update a student where we supply a where clause" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "gpa" => "decimal",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      update: [
        students_major: [
          students: %{
            "major" => "Bio",
            "dao@where" => "major = 'Biology'"
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expexted_results_str = get_expected_results_where_string()
    assert expexted_results_str == results
  end

  test "generate sql to update a student no where clause supplied" do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "gpa" => "decimal",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      update: [
        students_major: [
          students: %{
            "major" => "Bio"
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    assert get_expected_results_no_where_clause() == results
  end

  test "generate sql to where or " do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "gpa" => "decimal",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      update: [
        students_major: [
          students: %{
            "major" => "Biochemistry",
            "dao@where" => {{"major", "=", "Bio"}, "OR", {"major", "=", "Chemistry"}}
          },
          stugent: %{
            "name" => "Tom",
            "major" => "undecided",
            "dao@where" => {"student_id", "=", 1}
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)

    # nyd: this query must should show multiple queries generated or is it that the last operation will be the results but i think the track record of all queries must be availble
  end

  defp get_expected_results() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `students` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, major VARCHAR(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
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
            },
            "major" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            }
          }
        }
      },
      "root_cmd_node_list" => [
        update: [
          students_major: [
            students: %{
              "is_list" => true,
              "sql" =>
                "UPDATE `students` SET major = 'Bio' WHERE  WHERE (`students`.`major` = 'Biology') AND `students`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end

  defp get_expected_results_missing_cols() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "ALTER TABLE `students` ADD gpa DECIMAL(12,6), ADD major VARCHAR(30)"
        ],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "gpa" => %{
              "auto_increment" => false,
              "decimal_places" => 6,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "size" => 12,
              "sql" => "DECIMAL(12,6)",
              "type" => "decimal",
              "unique" => false
            },
            "major" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "name" => "string"
          }
        }
      },
      "root_cmd_node_list" => [
        update: [
          students_major: [
            students: %{
              "is_list" => true,
              "sql" =>
                "UPDATE `students` SET gpa = 23.5, major = 'Bio', name = 'Muwanguzi' WHERE  WHERE (`students`.`major` = 'Biology') AND `students`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end

  defp get_expected_results_single_and_condition() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "gpa" => "decimal",
            "major" => "string",
            "name" => "string"
          }
        }
      },
      "root_cmd_node_list" => [
        update: [
          students_major: [
            students: %{
              "is_list" => true,
              "sql" =>
                "UPDATE `students` SET major = 'Bio' WHERE  WHERE ((`students`.`major` = 'Biology') AND (`students`.`gpa` = 50)) AND `students`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end

  defp get_epected_results_or() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "gpa" => "decimal",
            "major" => "string",
            "name" => "string"
          }
        }
      },
      "root_cmd_node_list" => [
        update: [
          students_major: [
            students: %{
              "is_list" => true,
              "sql" =>
                "UPDATE `students` SET major = 'Bio' WHERE  WHERE (((`students`.`major` = 'Biology') AND (`students`.`gpa` = 50)) OR (`students`.`major` = 'Mathematics')) AND `students`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end

  defp get_expected_results_where_string() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "gpa" => "decimal",
            "major" => "string",
            "name" => "string"
          }
        }
      },
      "root_cmd_node_list" => [
        update: [
          students_major: [
            students: %{
              "is_list" => true,
              "sql" =>
                "UPDATE `students` SET major = 'Bio' WHERE  WHERE (major = 'Biology') AND `students`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end

  defp get_expected_results_no_where_clause() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "gpa" => "decimal",
            "major" => "string",
            "name" => "string"
          }
        }
      },
      "root_cmd_node_list" => [
        update: [
          students_major: [
            students: %{
              "is_list" => true,
              "sql" =>
                "UPDATE `students` SET major = 'Bio' WHERE  WHERE `students`.`is_deleted` = 0"
            }
          ]
        ]
      ]
    }
  end
end
