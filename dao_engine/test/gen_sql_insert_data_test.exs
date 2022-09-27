defmodule GenSqlInsertDataTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  setup do
    with {:ok, context} <- Dao.load_config_from_file("company_book_v_1_2") do
      %{
        "context" => context
      }
    else
      _ -> throw("Faild to read data dao config file ")
    end
  end

  test "gen sql insert data using arrays", %{"context" => context} do
    # INSERT INTO student VALUES(1, 'Jack', 'Biology')
    query = [
      add: [
        new_student: [
          student: [1, "Jack", "Biology"]
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = expected_results()
    assert expected_results == results
  end

  test "gen sql insert data using arrays with extra missing columns", %{"context" => context} do
    query = [
      add: [
        new_student: [
          student: [1, "Jack", "Biology", 45, true, "success", 34.5]
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = expected_results_missing_cols()
    assert expected_results == results
  end

  test "gen sql insert data using named columns", %{"context" => context} do
    query = [
      add: [
        new_student: [
          student: %{
            "student_id" => 1,
            "name" => "Kate"
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = expected_named_coloumns()
    assert expected_results == results
  end

  test "gen sql insert data using named columns missing columns", %{"context" => context} do
    query = [
      add: [
        new_student: [
          student: %{
            "student_id" => 1,
            "name" => "Kate",
            "average" => 45.6,
            "position" => 34,
            "is_ok" => false,
            "nick_name" => "sample boy"
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = expectd_names_missing_cols()
    assert expected_results == results
  end

  test "testing constrains when defininig columns", %{"context" => context} do
    context = %{context | "schema" => Map.delete(context["schema"], "students")}

    query = [
      add: [
        def_table: [
          student: %{
            "student_id" => "integer",
            "name" => %{
              "type" => "string",
              "size" => 20,
              "required" => true
            },
            "major" => %{
              "type" => "string",
              "size" => 20,
              "unique" => true
            },
            "dao@pks" => ["student_id"],
            "dao@use_default_pk" => false,
            "dao@timestamps" => false,
            "dao@def_only" => true
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = expected_results_unique_constrain()
    assert expected_results == results
  end

  test "testing more constrains when defininig columns", %{"context" => context} do
    context = %{context | "schema" => Map.delete(context["schema"], "students")}

    query = [
      add: [
        def_table: [
          student: %{
            "student_id" => %{
              "type" => "integer",
              "auto_increment" => true
            },
            "name" => %{
              "type" => "string",
              "size" => 20,
              "required" => true
            },
            "major" => %{
              "type" => "string",
              "size" => 20,
              "default" => "undecided"
            },
            "dao@pks" => ["student_id"],
            "dao@use_default_pk" => false,
            "dao@timestamps" => false,
            "dao@def_only" => true
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = expected_results_auto_increment()
    assert expected_results == results
  end

  def expected_results() do
    %{
      "context" => %{
        "accepted_named_queries" => [],
        "api" => nil,
        "auto_allowed_names_queries_changes" => [],
        "auto_alter_db" => true,
        "auto_alter_db_in_production" => false,
        "auto_schema_changes" => [],
        "database_name" => "company_book",
        "database_type" => "mysql",
        "environment" => "development",
        "migrations" => [],
        "multi_tenant" => false,
        "real_time" => true,
        "schema" => %{
          "students" => %{
            "dao@pks" => ["student_id"],
            "dao@timestamps" => false,
            "dao@use_default_pk" => false,
            "major" => "string",
            "name" => "string",
            "student_id" => "integer"
          }
        },
        "schema_timestamps" => true,
        "seeds" => [],
        "sudo_delete" => false,
        "transactional" => true,
        "pseudo_delete" => true
      },
      "root_cmd_node_list" => [
        add: [
          new_student: [
            student: %{
              "is_list" => false,
              "sql" => "INSERT INTO `students` VALUES(1, 'Jack', 'Biology')"
            }
          ]
        ]
      ]
    }
  end

  def expected_results_missing_cols() do
    %{
      "context" => %{
        "accepted_named_queries" => [],
        "api" => nil,
        "auto_allowed_names_queries_changes" => [],
        "auto_alter_db" => true,
        "auto_alter_db_in_production" => false,
        "auto_schema_changes" => [
          "ALTER TABLE `students` ADD col_4 INT(30), ADD col_5 TINYINT(1) NOT NULL DEFAULT 0, ADD col_6 VARCHAR(30), ADD col_7 DECIMAL(12,6)"
        ],
        "database_name" => "company_book",
        "database_type" => "mysql",
        "environment" => "development",
        "migrations" => [],
        "multi_tenant" => false,
        "real_time" => true,
        "schema" => %{
          "students" => %{
            "col_4" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            },
            "col_5" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => true,
              "size" => 1,
              "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
              "type" => "boolean",
              "unique" => false
            },
            "col_6" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "col_7" => %{
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
            "dao@pks" => ["student_id"],
            "dao@timestamps" => false,
            "dao@use_default_pk" => false,
            "major" => "string",
            "name" => "string",
            "student_id" => "integer"
          }
        },
        "schema_timestamps" => true,
        "seeds" => [],
        "sudo_delete" => false,
        "transactional" => true,
        "pseudo_delete" => true
      },
      "root_cmd_node_list" => [
        add: [
          new_student: [
            student: %{
              "is_list" => false,
              "sql" =>
                "INSERT INTO `students` VALUES(1, 'Jack', 'Biology', 45, true, 'success', 34.5)"
            }
          ]
        ]
      ]
    }
  end

  def expected_named_coloumns() do
    %{
      "context" => %{
        "accepted_named_queries" => [],
        "api" => nil,
        "auto_allowed_names_queries_changes" => [],
        "auto_alter_db" => true,
        "auto_alter_db_in_production" => false,
        "auto_schema_changes" => [],
        "database_name" => "company_book",
        "database_type" => "mysql",
        "environment" => "development",
        "migrations" => [],
        "multi_tenant" => false,
        "real_time" => true,
        "schema" => %{
          "students" => %{
            "dao@pks" => ["student_id"],
            "dao@timestamps" => false,
            "dao@use_default_pk" => false,
            "major" => "string",
            "name" => "string",
            "student_id" => "integer"
          }
        },
        "schema_timestamps" => true,
        "seeds" => [],
        "sudo_delete" => false,
        "transactional" => true,
        "pseudo_delete" => true
      },
      "root_cmd_node_list" => [
        add: [
          new_student: [
            student: %{
              "is_list" => false,
              "sql" => "INSERT INTO `students`(name, student_id) VALUES('Kate', 1)"
            }
          ]
        ]
      ]
    }
  end

  def expectd_names_missing_cols() do
    %{
      "context" => %{
        "accepted_named_queries" => [],
        "api" => nil,
        "auto_allowed_names_queries_changes" => [],
        "auto_alter_db" => true,
        "auto_alter_db_in_production" => false,
        "auto_schema_changes" => [
          "ALTER TABLE `students` ADD average DECIMAL(12,6), ADD is_ok TINYINT(1) NOT NULL DEFAULT 0, ADD nick_name VARCHAR(30), ADD position INT(30)"
        ],
        "database_name" => "company_book",
        "database_type" => "mysql",
        "environment" => "development",
        "migrations" => [],
        "multi_tenant" => false,
        "real_time" => true,
        "schema" => %{
          "students" => %{
            "average" => %{
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
            "dao@pks" => ["student_id"],
            "dao@timestamps" => false,
            "dao@use_default_pk" => false,
            "is_ok" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => true,
              "size" => 1,
              "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
              "type" => "boolean",
              "unique" => false
            },
            "major" => "string",
            "name" => "string",
            "nick_name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "position" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            },
            "student_id" => "integer"
          }
        },
        "schema_timestamps" => true,
        "seeds" => [],
        "sudo_delete" => false,
        "transactional" => true,
        "pseudo_delete" => true
      },
      "root_cmd_node_list" => [
        add: [
          new_student: [
            student: %{
              "is_list" => false,
              "sql" =>
                "INSERT INTO `students`(average, is_ok, name, nick_name, position, student_id) VALUES(45.6, false, 'Kate', 'sample boy', 34, 1)"
            }
          ]
        ]
      ]
    }
  end

  def expected_results_unique_constrain() do
    %{
      "context" => %{
        "accepted_named_queries" => [],
        "api" => nil,
        "auto_allowed_names_queries_changes" => [],
        "auto_alter_db" => true,
        "auto_alter_db_in_production" => false,
        "auto_schema_changes" => [
          "CREATE TABLE `students` (major VARCHAR(20) UNIQUE, name VARCHAR(20) NOT NULL, student_id INT(30), PRIMARY KEY(student_id))"
        ],
        "database_name" => "company_book",
        "database_type" => "mysql",
        "environment" => "development",
        "migrations" => [],
        "multi_tenant" => false,
        "real_time" => true,
        "schema" => %{
          "students" => %{
            "major" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 20,
              "sql" => "VARCHAR(20) UNIQUE",
              "type" => "string",
              "unique" => true
            },
            "name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => true,
              "size" => 20,
              "sql" => "VARCHAR(20) NOT NULL",
              "type" => "string",
              "unique" => false
            },
            "student_id" => %{
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
        "schema_timestamps" => true,
        "seeds" => [],
        "sudo_delete" => false,
        "transactional" => true,
        "pseudo_delete" => true
      },
      "root_cmd_node_list" => [
        add: [def_table: [student: %{"is_list" => false, "sql" => ""}]]
      ]
    }
  end

  def expected_results_auto_increment() do
    %{
      "context" => %{
        "accepted_named_queries" => [],
        "api" => nil,
        "auto_allowed_names_queries_changes" => [],
        "auto_alter_db" => true,
        "auto_alter_db_in_production" => false,
        "auto_schema_changes" => [
          "CREATE TABLE `students` (major VARCHAR(20) DEFAULT 'undecided', name VARCHAR(20) NOT NULL, student_id INT(30) AUTO_INCREMENT, PRIMARY KEY(student_id))"
        ],
        "database_name" => "company_book",
        "database_type" => "mysql",
        "environment" => "development",
        "migrations" => [],
        "multi_tenant" => false,
        "real_time" => true,
        "schema" => %{
          "students" => %{
            "major" => %{
              "auto_increment" => false,
              "default" => "undecided",
              "is_primary_key" => false,
              "required" => false,
              "size" => 20,
              "sql" => "VARCHAR(20) DEFAULT 'undecided'",
              "type" => "string",
              "unique" => false
            },
            "name" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => true,
              "size" => 20,
              "sql" => "VARCHAR(20) NOT NULL",
              "type" => "string",
              "unique" => false
            },
            "student_id" => %{
              "auto_increment" => true,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30) AUTO_INCREMENT",
              "type" => "integer",
              "unique" => false
            }
          }
        },
        "schema_timestamps" => true,
        "seeds" => [],
        "sudo_delete" => false,
        "transactional" => true,
        "pseudo_delete" => true
      },
      "root_cmd_node_list" => [
        add: [def_table: [student: %{"is_list" => false, "sql" => ""}]]
      ]
    }
  end
end
