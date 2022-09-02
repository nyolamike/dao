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

    results = Dao.execute(context, query)
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

    results = Dao.execute(context, query)
    expected_results = expected_results_missing_cols()
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
        "transactional" => true
      },
      "root_cmd_node_list" => [
        add: [
          new_student: [
            student: %{
              "is_list" => false,
              "sql" => "INSERT INTO `company_book.students` VALUES(1, 'Jack', 'Biology')"
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
        "auto_schema_changes" => ["ALTER TABLE `company_book.students` ADD col_4 INT(30), ADD col_5 TINYINT(1) NOT NULL DEFAULT 0, ADD col_6 VARCHAR (30), ADD col_7 DECIMAL(12,6)"],
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
              "sql" => "INT(30)  ",
              "type" => "integer"
            },
            "col_5" => %{
              "auto_increment" => false,
              "default" => nil,
              "is_primary_key" => false,
              "required" => true,
              "size" => 1,
              "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
              "type" => "string"
            },
            "col_6" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR (30)  ",
              "type" => "string"
            },
            "col_7" => %{
              "auto_increment" => false,
              "decimal_places" => 6,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "size" => 12,
              "sql" => "DECIMAL(12,6)  ",
              "type" => "decimal"
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
        "transactional" => true
      },
      "root_cmd_node_list" => [
        add: [
          new_student: [
            student: %{
              "is_list" => false,
              "sql" => "INSERT INTO `company_book.students` VALUES(1, 'Jack', 'Biology', 45, true, 'success', 34.5)"
            }
          ]
        ]
      ]
    }
  end
end
