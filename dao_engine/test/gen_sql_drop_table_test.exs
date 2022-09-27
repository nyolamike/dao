defmodule GenSqlDropTableTest do
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

  test "generate sql to delete a table", %{"context" => context} do
    query = [
      delete: [
        students_table: [
          students: "table"
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = get_expected_results()

    assert expected_results == results
  end

  test "generate sql to add a coloumn to a table", %{"context" => context} do
    # vidoe example
    # ALTER TABLE student ADD gpa DECIMAL(3,2);

    # nyd: if this array is not empty and the insert sql has not info them no insert sql is generated
    # nested insert queries e.g in shopping carts scenarios
    # context = Map.put(context,"schema", %{})
    query = [
      add: [
        students_gender_col: [
          students: %{
            "gpa" => %{
              "type" => "float",
              "size" => 3,
              "decimal_places" => 2
            },
            "dao@def_only" => true
          }
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    epected_results = get_expected_results_gpa()
    assert epected_results == results
  end

  test "generate sql to drop coloumn from a table", %{"context" => context} do
    # vidoe example
    # ALTER TABLE student DROP COLUMN gpa;

    schema = %{
      context["schema"]
      | "students" => Map.put(context["schema"]["students"], "gpa", "string")
    }

    context = %{context | "schema" => schema}
    # nyd: find out how this affects relationships, pks and schema
    query = [
      delete: [
        students_gpa_col: [
          students: ["gpa"]
        ]
      ]
    ]

    results = Dao.translate_query(context, query)
    expected_results = get_expected_results_delete_gpa()
    assert expected_results == results
  end

  def get_expected_results() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => ["dao@skip: DROP TABLE `students`"],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{}
      },
      "root_cmd_node_list" => [
        delete: [
          students_table: [
            students: %{"is_list" => true, "sql" => "DROP TABLE `students`"}
          ]
        ]
      ]
    }
  end

  def get_expected_results_gpa() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "dao@skip: ALTER TABLE `students` ADD gpa DECIMAL(3,2)"
        ],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "gpa" => %{
              "auto_increment" => false,
              "decimal_places" => 2,
              "default" => nil,
              "is_primary_key" => false,
              "required" => "",
              "size" => 3,
              "sql" => "DECIMAL(3,2)",
              "type" => "decimal",
              "unique" => false
            },
            "name" => "string"
          }
        }
      },
      "root_cmd_node_list" => [
        add: [
          students_gender_col: [
            students: %{
              "is_list" => true,
              "sql" => "ALTER TABLE `students` ADD gpa DECIMAL(3,2)"
            }
          ]
        ]
      ]
    }
  end

  def get_expected_results_delete_gpa() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => ["dao@skip: ALTER TABLE `students` DROP COLUMN gpa"],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"name" => "string"}}
      },
      "root_cmd_node_list" => [
        delete: [
          students_gpa_col: [
            students: %{
              "is_list" => true,
              "sql" => "ALTER TABLE `students` DROP COLUMN gpa"
            }
          ]
        ]
      ]
    }
  end

  test "generate sql to add a coloumn to a table considering some other variations", %{
    "context" => context
  } do
    # nyd: enable this test
    # nyd: if this array is not empty and the insert sql has not info them no insert sql is generated
    # nested insert queries e.g in shopping carts scenarios
    # context = Map.put(context,"schema", %{})
    # query = [
    #   add: [
    #     students_gender_col: [
    #       students: %{
    #         "gender" => "string",
    #         "points" => "integer",
    #         "fake_pk_id" => "pk",
    #         "dao@pks" => ["points"],
    #         "dao@use_default_pk" => false,
    #         "dao@timestamps" => false,
    #         "dao@skip_insert" => ["gender"],
    #       }
    #     ]
    #   ]
    # ]

    # # query = [
    # #   add: [
    # #     students_gender_col: [
    # #       students: %{
    # #         "columns" => %{
    # #           "col_gender" => "string",
    # #           "points" => "integer",
    # #           "fake_pk_id" => "pk",
    # #         }
    # #       }
    # #     ]
    # #   ]
    # # ]

    # results = Dao.translate_query(context, query)
    # IO.inspect(results)
  end
end
